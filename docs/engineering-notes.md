# Engineering Notes

README.mdでは扱いきれなかった実装の詳細・デバッグの記録。面接等での深掘りに対するネタ元、または今後同種の構成に取り組む際の参考用。

---

## ディレクトリ構成

```
.
├── terraform/
│   ├── modules/
│   │   ├── aks/                # AKSクラスタ本体
│   │   ├── event_hubs/         # Event Hubs namespace + Kafka互換設定
│   │   ├── adls2/              # ストレージアカウント + コンテナ
│   │   ├── networking/         # VNet, Subnet
│   │   └── acr/                # sql-runnerイメージ置き場、AKSにAcrPull付与
│   └── env/
│       └── dev/                # `terraform output`で各種接続情報を取得
├── k8s/
│   ├── flink-operator/         # Flink Kubernetes OperatorのHelmインストール
│   ├── polaris/                # Polaris本体（Deployment + Service、in-memory）
│   └── flink-deployment/       # FlinkDeployment CRD + secretsからのenvsubst&apply
├── flink-jobs/
│   ├── inventory-monitor/      # 在庫減少検知ジョブ（4本のFlink SQL）
│   └── sql-runner/             # SQLファイルを順に実行する自作Javaランナー
├── simulator/
│   └── inventory-event-producer/  # 在庫変動イベントのシミュレータ（Python）
├── scripts/                    # 動作確認・メンテナンス用（後述）
└── .github/workflows/          # terraform apply/destroy、Icebergメンテナンス
```

---

## どのスクリプトが何を作るか

スクリプトはすべて手元PCで実行し、`kubectl`/`helm`/`az`経由でAKSに指示を出す。**AKSの中でスクリプトは動かない**。

AKSに最終的にできるもの（すべてPod）:

```
flinkの名前空間
 ├─ flink-kubernetes-operator      Flinkジョブを管理する番人
 ├─ polaris                        Iceberg REST Catalogサーバー
 └─ inventory-monitor              JobManager / TaskManager（FlinkDeploymentからOperatorが生成）
cert-manager（3 Pod）              Operatorのwebhook用の証明書発行
```

| スクリプト | 何をするか | 結果 |
|---|---|---|
| `k8s/flink-operator/install.sh` | cert-manager と Flink Kubernetes Operator を導入 | Operator Pod、`FlinkDeployment` CRD |
| `flink-jobs/sql-runner/build-and-push.sh` | sql-runnerをビルドしACRへpush | ACR上のイメージ（Podはまだできない） |
| `k8s/flink-deployment/01_render-and-deploy.sh` | 秘密情報をSQLに埋めてConfigMapと`FlinkDeployment`を適用 | Operatorが JobManager/TaskManager Pod を生成 |
| `scripts/setup-polaris.sh` | 起動済みPolarisにカタログとFlink用principalを登録 | Podは増えない。Polarisの中身が入る |
| `scripts/setup-oidc.sh` | GitHub Actions用のOIDC認証をAzureに登録（初回のみ） | Azure ADのアプリ登録 |

Polaris自体のデプロイはスクリプトではなく、`k8s/polaris/`のYAMLを`kubectl apply`する。なお「flink」は、Kubernetesの名前空間（Polarisも同居）・Flinkのソフト本体・`flink-jobs/`ディレクトリの3つの意味で使っている。

---

## 動かし方（詳細手順）

検証セッションごとにインフラを作って壊す運用（コスト管理のため）。手順は実機で通した順序:

1. **インフラをapply**
   ```bash
   cd terraform/env/dev
   cp dev.tfvars.example dev.tfvars   # 自宅IPを記入
   terraform apply -var-file=dev.tfvars
   ```
2. **kubectlをAKSに接続**
   ```bash
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) \
     --name $(terraform output -raw aks_cluster_name)
   ```
3. **Flink Kubernetes Operatorをインストール**: `k8s/flink-operator/install.sh`（cert-managerも入る）
4. **Polarisをデプロイ**: `k8s/polaris/02_secret.example.yaml`を`02_secret.yaml`にコピーして認証情報を埋め、`POLARIS_WORKLOAD_IDENTITY_CLIENT_ID`（`terraform output -raw polaris_workload_identity_client_id`）を環境変数にセットしてから
   ```bash
   POLARIS_WORKLOAD_IDENTITY_CLIENT_ID=$(terraform output -raw polaris_workload_identity_client_id) \
     k8s/polaris/00_render-and-deploy.sh
   ```
5. **Polarisを初期化**: `kubectl port-forward svc/polaris 8181:8181 -n flink`を開いた状態で`scripts/setup-polaris.sh`。カタログ`lakehouse`とFlink専用のprincipal（`flink_app`）を作り、その認証情報を出力する。in-memoryなのでPolarisのPodが再起動したら再実行する
6. **SQLランナーをビルド・push**: `ACR_NAME=... SQL_RUNNER_TAG=<一意なタグ> flink-jobs/sql-runner/build-and-push.sh`（タグは毎回変える）
7. **FlinkDeploymentをデプロイ**: `k8s/flink-deployment/00_secrets.example.env`を`00_secrets.env`にコピーし、`terraform output`の値（`ADLS_ACCOUNT_NAME`・`flink_workload_identity_client_id`）・手順5の認証情報・手順6のタグを埋めてから`k8s/flink-deployment/01_render-and-deploy.sh`
8. **シミュレータでイベントを流す**: `simulator/inventory-event-producer/producer.py`（狙った商品だけ動かしたい場合は`send_demo_events.py <product_id>`）
9. **動作確認**: port-forwardを開いた別ターミナルで`scripts/verify_stock_status.py`、または実データをそのまま見たい場合は`scripts/verify_stock_status_duckdb.sh`
10. **後片付け**: `terraform destroy -var-file=dev.tfvars`

GitHub Actions（`terraform_apply.yml`/`terraform_destroy.yml`）からも1・10はworkflow_dispatchで実行できる（OIDC認証、`scripts/setup-oidc.sh`で事前セットアップが必要）。

---

## 個別のバグ・詰まりどころの記録

**なぜEvent HubsをKafka protocol互換で使うのか（Azure純正のSDKではなく）**
FlinkのKafka Connectorをそのまま使え、追加の専用コネクタ実装が要らないため。カタログ（Polaris）は出口コストを理由に選んでいるが、すべてのレイヤーで脱ベンダーを目指しているわけではなく、メッセージングのようにマネージドの恩恵が大きいレイヤーは素直にAzureのサービスを使う、という使い分け。

**環境分離はどの単位で行うか**
Azureの本番組織ではサブスクリプションをdev/stg/prdで分離するのが定石だが、単一環境で完結する本ポートフォリオでは1サブスクリプション内のリソースグループ分離を採用。定石を知った上での意図的な簡略化。

**Icebergのmetadata/manifest/data fileが際限なく増える問題にどう対応するか**
Icebergはcheckpointのたびに新しいmetadata.jsonを追加する（上書きしない）仕様のため、放置すると本番運用ではファイル数が容易に数千〜数万に達する。この対策として`scripts/expire_snapshots.py`と`.github/workflows/iceberg_maintenance.yml`でExpire Snapshotsを実装している。本ポートフォリオの実際の運用（検証セッションごとに`terraform destroy`でADLS2ごと環境を破棄する）では蓄積は1セッション分にしか発生しないが、本番運用でこの問題が実際に起きること・その対処法を理解していることを示すために実装した。

**ADLS2へのネットワーク経路とアクセス認証はどこまで本番相当か**
ADLS2は`public_network_access_enabled = true`のまま、ファイアウォールを`default_action = "Deny"`にしてAKSのサブネット（サービスエンドポイント）と検証用の自宅IPだけを許可している。本番ならPrivate Endpointで公開エンドポイントを完全に閉じるのが正しいが、検証スクリプトを手元PCから実行できる構成を優先し、定石を知った上で簡略化している。

**Azure従量課金でVMサイズをどう選ぶか（quotaとSKU制限）**
ノードVMは`Standard_D2as_v7`。当初の`Standard_B2s_v2`は`ErrCode_InsufficientVCPUQuota`で作成に失敗した。vCPU quotaはリージョン合計とは別に**VMファミリーごと**に割り当てられ、この従量課金サブスクリプションではBsv2やDsv5が0だった。さらにquotaがあっても`NotAvailableForSubscription`（SKU制限）で使えないサイズがあり、**両方を満たすものだけが使える**。無料試用では通っていたため`plan`でも気付けない。`az vm list-usage`と`az vm list-skus`の突き合わせで選定した。

**コンテナイメージのタグはなぜ毎回変えるのか**
`:latest`のようなmutableなタグは、ノードが`imagePullPolicy: IfNotPresent`でキャッシュするため、ACRに再pushしても古いイメージのまま動き続ける（実際に発生し、原因特定に時間を要した）。ビルドごとに一意なタグ（`SQL_RUNNER_TAG`）を明示し、`build-and-push.sh`はタグ無しでは実行を拒否する。

**課金環境で試す前に、何を手元で検証するか**
Polarisの設定は、AKSに載せる前に手元のDockerで同じ手順（起動、認証、カタログ作成、名前空間作成）を通した。これで公式ドキュメントに載っていない挙動（realm名の不一致は`unauthorized_client`としか返らない、`default-base-location`はコンテナのルートでなければ名前空間作成が400になる）を、課金なしで潰せた。Flink側も同様に、`Could not find any factory for identifier 'iceberg'`という「ファクトリが存在しない」ように見えるエラーが、実際には**jarが読めない・依存クラスが読めないためにファクトリが発見対象から静かに除外された**場合にも出る。今回の根本原因は、DockerfileのADDで入れたjarが所有者root・権限600になり、`flink`ユーザーで動くJobManagerが読めなかったこと。メッセージだけでは区別できないため、原因が分からないときは課金環境で粘らず、手元で最小構成に落として切り分ける、という方針を徹底した。

**PolarisへのOAuthで`invalid_scope`になるのはなぜか**
IcebergのRESTクライアントは、認証時に既定でscope `catalog`を送るが、Polarisはこれを拒否し`PRINCIPAL_ROLE:ALL`（または特定のprincipal role）を要求する。FlinkのSQLでは`'scope' = 'PRINCIPAL_ROLE:ALL'`を指定する。

**event_timeの型はなぜ`TIMESTAMP_LTZ(3)`で、シミュレータはなぜ`Z`サフィックスを送るのか**
Flinkの`json.timestamp-format.standard = 'ISO-8601'`は、タイムゾーン付きの値を`TIMESTAMP_LTZ`列に読ませる前提で、**`Z`サフィックスしか受け付けない**。Pythonの`datetime.isoformat()`が出す`+00:00`のようなオフセット表記は、**エラーにならず黙って`NULL`になる**（`json.ignore-parse-errors`を使わないと気付けない罠）。手元でJobManager+TaskManagerの組を立てて複数の表記を試し、特定した。

**`verify_stock_status.py`がテーブルを読めないことがあるのはなぜか**
`03_sink.sql`の`write.upsert.enabled=true`により、Flinkの`IcebergSink`は既存の`product_id`を更新するたびequality delete形式の削除ファイルを書く。pyiceberg（0.12.0、2026-09時点の最新）はこの形式のdeleteをまだマージして読めず（[apache/iceberg#6568](https://github.com/apache/iceberg/issues/6568)）、`table.scan()`が`ValueError`を投げる。データ自体は正しくコミットされているため（`table.metadata.snapshots`で確認可能）、`verify_stock_status.py`はこの例外を捕捉し、スナップショット／マニフェストのメタデータ確認にフォールバックする実装にしている。

**CI用Service PrincipalのIAMロールを、なぜContributorだけでは足りずUser Access Administratorも要るのか**
`setup-oidc.sh`で作るCI用Service Principalには、最初サブスクリプションスコープの`Contributor`だけを付与していたが、GitHub Actionsから実際に`terraform apply`を実行すると2箇所で失敗した。①`terraform init`が`AuthorizationPermissionMismatch`でtfstateバックエンド（`use_azuread_auth = true`）にアクセスできない — `Contributor`は管理プレーンの権限であり、Azure ADトークンでのBlobデータ読み書き（データプレーン）には別途`Storage Blob Data Contributor`のようなデータプレーンロールが要る（Polaris自身のIAM問題と同型のバグ）。②AKS kubelet identityへの`azurerm_role_assignment`作成が`AuthorizationFailed`で失敗 — `Contributor`は意図的に`Microsoft.Authorization/roleAssignments/write`（他者への権限付与）を含まない設計になっており、Terraform自身がIAMロールを付与するコードを含む場合は`User Access Administrator`（または`Owner`）が別途必要。

**`upgradeMode: last-state`と`high-availability`をセットで入れた理由**
`upgradeMode: stateless`は、FlinkDeploymentを再適用（redeploy）するたびに直前のチェックポイントを無視して完全にゼロから起動する。デバッグ中に何度も`kubectl delete flinkdeployment && kubectl apply`を繰り返した際、この挙動により集計状態（現在庫の累計）が毎回リセットされるのを実際に目撃した。`last-state`に変えると、redeploy時に直前の実行から自動的に再開する。ただし`last-state`はFlink自身のHA機構（`high-availability.type: kubernetes`）が有効になっていないと黙って`stateless`と同じ動作になるため、`high-availability.storageDir`とセットで設定した。実機で確認済み：`execution.checkpointing.interval`を変えて（`kubectl delete`せず）再適用したところ、JobManagerのログに`Restoring job <同一jobId> from Checkpoint 35`と出力され、同一のjob ID・チェックポイント番号の連番継続・Kafkaソースのoffset位置すべてが引き継がれることを確認した。

**shared keyからWorkload Identityへの移行時に踏んだライブラリバージョンの壁**
Flink自身のチェックポイント/HA（Hadoop ABFSドライバ経由）はshared keyのまま残した。これは実機を使わず、jarの中身を直接調べて分かった制約：このDockerイメージが使う`flink-azure-fs-hadoop-1.20.5.jar`は2022年ビルドの古いHadoop-Azureドライバを内蔵しており、`WorkloadIdentityTokenProvider`クラスが存在しない（Maven Central最新の`hadoop-azure:3.4.1`には存在することを確認済み）。単純にjarを新しいバージョンに差し替えると、同じjarに同居しているFlink側の連携クラスまで失う可能性があり、安全に置き換えられないため、この部分だけkeyless化を見送った。

**pyicebergの`expire_snapshots`、正しいAPIはどこにあるのか**
`Table.expire_snapshots()`は存在しない（0.12.0時点）。実際のエントリポイントは`Table.maintenance.expire_snapshots()`（`ExpireSnapshots`ビルダーを返す）で、`.older_than(dt)`はエポックミリ秒ではなく`datetime`オブジェクトを要求する。ドキュメントよりインストール済みパッケージ（`pyiceberg/table/maintenance.py`）を直接読んで確認した。

**デモで「今のテーブルの中身」を素直に見せるための、読む側ツールの切り替え**
pyicebergがequality deleteを読めない制約に対し、Flink側の集計方式を`GROUP BY`のupsertから`OVER`ウィンドウの追記型に変える案も検討したが、書き込みパイプラインの設計変更で影響範囲が大きい。デモの実際の要求は「テーブルの中身をそのまま見せたい」だけだったため、読む側のツールをpyicebergからDuckDBに変える方針に転換した（`scripts/verify_stock_status_duckdb.sh`）。DuckDBの`iceberg`拡張はequality deleteをマージして読める。Flink SQL側（`01_catalog.sql`/`03_sink.sql`/`04_pipeline.sql`）は一切変更していない。実機では既定のAzure SDKトランスポートで`Problem with the SSL CA cert`エラーが発生したが（システムのCA証明書自体は正常、`curl`では同エンドポイントに繋がる）、`SET azure_transport_option_type = 'curl';`で解決した。

**Icebergメンテナンスのworkflow_dispatchが失敗する理由**
`iceberg_maintenance.yml`は`az aks get-credentials`までは成功するが、続く`kubectl port-forward`が`ConnectionRefusedError`で失敗する。原因はAKSのAPIサーバー自体が`authorized_ip_ranges`（自宅IPのみ）でファイアウォールされていること。`az aks get-credentials`はARM（管理プレーン）呼び出しなので通るが、`kubectl`はAPIサーバーへの直接接続が要り、GitHub-hostedランナーは実行のたびに異なるIPを使うため許可リストに引っかかる。同じ理由は`terraform_apply.yml`/`terraform_destroy.yml`には当てはまらない（Terraformが触るのはARM APIのみで、AKSのAPIサーバーには一切接続しないため）。直すには実行前後で`authorized_ip_ranges`を一時的に広げる、あるいはVNet内にself-hosted runnerを置く必要があるが、優先度が低いため見送り、`scripts/expire_snapshots.py`は手元から手動実行する運用と割り切った。
