# Azure Realtime Lakehouse

在庫減少をリアルタイムに検知するストリーミング基盤。Azure Event Hubs → Flink on AKS → Apache Iceberg（ADLS2 + Apache Polaris）という構成で、「日次バッチでは検知が翌朝になる」という遅延を解消することにフォーカスしたポートフォリオです。

---

## なぜこれをやるのか

従来の在庫管理は日次バッチ集計が前提になっていることが多く、当日発生した欠品は翌朝のバッチ実行まで検知されません。その間、

- **機会損失**：商品があれば売れていたはずの売上を、検知が遅れた分だけ失う
- **過剰在庫リスク**：逆に「欠品が怖いから多めに持つ」という安全マージンの取り方になりがちで、倉庫コストや売れ残りリスクを抱え込む

という、攻め（機会損失）と守り（在庫コスト）の両方にトレードオフが生じます。ストリーミングで在庫変動をリアルタイムに検知できれば、検知ラグが縮む分だけこのトレードオフ自体を緩和できる、というのがこのプロジェクトの仮説です。

### スコープ

このリポジトリが実装するのは、**在庫減少のリアルタイム検知〜Icebergテーブルへの格納までのストリーミング基盤部分**です。検知結果を受けた自動発注ロジックや、発注先システムとの連携は対象外です。あくまで「リアルタイム性のある検知基盤を、本番運用を想定した構成（IaC化・K8s運用）で構築できる」ことの証明が目的です。

シミュレータによる合成データを使うため、「機会損失○％削減」のような効果の定量化は行いません（行えません）。目的はそこではなく、「欠品を恐れて在庫を過剰に抱える」「そのせいで倉庫を余分に確保し続ける」といった、検知の遅さに起因する現状からの解放です。定量効果ではなく、その現状を変えうる検知基盤を実際に動く形で示すことがゴールです。

---

## アーキテクチャ

![Architecture](img/architecture.png)

- **Event Hubs**：Kafka protocol互換エンドポイントを使い、Flinkからは通常のKafkaソースとして接続する
- **Flink on AKS**：Flink Kubernetes OperatorでFlinkDeployment CRDとしてジョブを管理。商品ごとに現在庫数をstateとして保持し、イベントごとに即時更新・閾値判定（詳細はADR参照）した結果をIcebergにsink
- **ADLS2 + Polaris**：データ実体はADLS2に、テーブルの最新状態（metadata.jsonへのポインタ）はPolarisが管理。クラウド中立なREST Catalog仕様で構築し、特定ベンダーへのロックインを避ける

検知結果の確認・動作検証は、別途クエリエンジンを立てず`pyiceberg`等によるカタログ越しの直接読み出しで行う（コンポーネントを増やさない方針。詳細はADR参照）。

---

## 技術スタック

| レイヤ | 技術 |
|---|---|
| メッセージング | Azure Event Hubs（Kafka protocol互換） |
| ストリーム処理 | Apache Flink（Flink Kubernetes Operator） |
| コンテナ基盤 | Azure Kubernetes Service (AKS) |
| ストレージ | Azure Data Lake Storage Gen2 (ADLS2) |
| テーブルフォーマット | Apache Iceberg |
| カタログ | Apache Polaris（Iceberg REST Catalog） |
| インフラ | Terraform |
| CI/CD | GitHub Actions |

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

## 動かし方

検証セッションごとにインフラを作って壊す運用（コスト管理のため）。おおまかな手順:

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
3. **Flink Kubernetes Operatorをインストール**: `k8s/flink-operator/install.sh`
4. **Polarisをデプロイ**: `k8s/polaris/01_secret.example.yaml`を`01_secret.yaml`にコピーして値を埋め、`kubectl apply -f k8s/polaris/`
5. **SQLランナーをビルド・push**: `ACR_NAME=$(terraform output -raw acr_login_server | cut -d. -f1) flink-jobs/sql-runner/build-and-push.sh`
6. **FlinkDeploymentをデプロイ**: `k8s/flink-deployment/00_secrets.example.env`を`00_secrets.env`にコピーし、`terraform output`の値とPolarisのbootstrap認証情報を埋めてから`k8s/flink-deployment/01_render-and-deploy.sh`
7. **シミュレータでイベントを流す**: `simulator/inventory-event-producer/producer.py`
8. **動作確認**: `kubectl port-forward svc/polaris 8181:8181 -n flink`を開いた別ターミナルで`scripts/verify_stock_status.py`
9. **後片付け**: `terraform destroy -var-file=dev.tfvars`

GitHub Actions（`terraform_apply.yml`/`terraform_destroy.yml`）からも1・9はworkflow_dispatchで実行できる（OIDC認証、`scripts/setup-oidc.sh`で事前セットアップが必要）。

---

## 設計判断（ADR）

**なぜAzure純正のカタログ（Fabric Catalogなど）ではなくApache Polarisを使うのか？**
目指しているのは「ベンダー中立」ではなく「移行容易性」。何かに依存すること自体は避けられないので、依存先を変えたくなったときに同じ仕組みを別の手段で用意し直せるか（出口コスト）を判断基準にしている。データ実体はIceberg仕様のファイル群としてADLS2にあり、ストレージ間コピーで動かせる。しかしカタログをAzure独自仕様にすると、この一番重い資産の「正」の管理だけが特定ベンダーに癒着し、出口を塞ぐ。REST Catalog仕様準拠のPolarisなら、仮にPolaris自体が廃れても同仕様の別実装へポインタを載せ替えるだけで移行できる。結果として本構成は、メッセージング（Kafka protocol）・コンテナ基盤（K8s API）・テーブル（Iceberg spec）・カタログ（REST Catalog仕様）の各レイヤーが標準仕様を境界面として持ち、Azureのマネージドに依存しながらも出口コストが有界になっている。

**なぜFlink Kubernetes Operatorを使うのか（Standaloneモードではなく）？**
Operatorを使うとFlinkDeploymentというCRDでジョブをKubernetesネイティブに管理でき、デプロイ・スケーリング・障害復旧がkubectl/Terraform経由で完結する。実務でのAKS運用力を証明するという目的上、K8sネイティブな運用フローを採用する方が説得力がある。

**なぜEvent HubsをKafka protocol互換で使うのか（Azure純正のSDKではなく）？**
FlinkのKafka Connectorをそのまま使え、追加の専用コネクタ実装が要らないため。ポータビリティが主目的ではなく、AzureのマネージドサービスとしてEvent Hubsを使いながら、Flink側の実装をKafka標準のままにできる利便性が理由。カタログ（Polaris）は出口コストを理由に選んでいるが、すべてのレイヤーで脱ベンダーを目指しているわけではなく、メッセージングのようにマネージドの恩恵が大きいレイヤーは素直にAzureのサービスを使う、という使い分け。Kafka protocolで接続している結果として、ここも出口（他のKafka互換サービスへの移行）は確保されている。

**Kafka offsetとIcebergのcommitがズレて重複・欠落が起きないか？（exactly-once保証）**
FlinkはKafkaソースのoffsetとIcebergへの書き込みコミットを、checkpoint機構で同期させる。具体的には、checkpoint開始時にKafka offsetをスナップショットし、checkpoint完了（バリアがsinkまで到達)時に初めてIcebergの新しいsnapshotをcommitする2相コミット的な仕組みになっている。checkpoint失敗時はそのcheckpoint時点のoffsetまで巻き戻して再処理するため、Iceberg側には未完了のcommitが残らず、結果的にexactly-onceが成立する。この仕組みは事前にローカル検証（kafka-flink-iceberg-handson）でcheckpoint前後のmanifest/snapshotファイルの増え方を実際に確認済み。

**なぜ検知結果の保存先にPostgresのようなDBではなくIcebergを使うのか？**
検知結果を溜めるだけならPostgresでも要件は満たせる。しかしPostgresを使うと「常時起動が必要なマネージドDBサービス」がもう一つ増えることになり、運用コンポーネントが無駄に増える。Icebergはストレージ（ADLS2）＋カタログ（Polaris）だけで完結し、専用のDBサーバを持たない。検知結果をオンデマンドで読みたいだけなら、ストレージ層だけで十分という判断。

**なぜTrinoのような別のクエリエンジンを立てないのか？**
動作確認だけが目的であれば、`pyiceberg`等でPolarisカタログ経由でテーブルを直接読めば足りる。Trinoを常時稼働させるのは「検証用」の名目に対してコンポーネントが過剰で、AKSの運用コストも増える。クエリエンジンを増やすメリット（複数エンジンからの同時アクセスの証明）よりコストの方が大きいと判断し、スコープから外した。

**検知ロジックはウィンドウ集計かstateful processingか？**
stateful processing（`KeyedProcessFunction`で商品IDごとに現在庫数を保持し、イベントごとに即時更新・閾値判定）を採用する。在庫数は「期間内の変化量」ではなく「今この瞬間の値」なので、ウィンドウ集計（期間で区切ってから判定）とは表現したいものの構造が合わない上、ウィンドウが閉じるまで判定を待つ遅延が再び発生し、「即座に検知する」という本プロジェクトの前提と矛盾する。

なお閾値そのもの（何個を下回ったらアラートか）の最適値を導出するロジックはスコープ外。これは在庫最適化（需要予測・発注リードタイムを踏まえた発注点計算）の問題であり、ストリーミング基盤の役割は「外部から設定された閾値を、設定が何であれ即座に検知できること」に限定する。

**環境分離はどの単位で行うか？**
Azureの本番組織ではサブスクリプションをdev/stg/prdで分離し、Management Groupでガバナンスを統一するのが定石。ただし単一環境で完結する本ポートフォリオでは、1サブスクリプション内のリソースグループ分離（`rg-realtime-lakehouse-dev`）を採用する。定石を知った上での意図的な簡略化であり、Terraformの`env/`構造は環境が増えた場合にそのまま拡張できる形にしておく。

**常時稼働させるのか？コストはどう考えているか？**
ポートフォリオ規模なので基本は検証時のみの稼働。ただしストリーミング処理という性質上、常時起動していること自体に意味があるため、24時間365日ではなく「営業時間内（在庫イベントが発生する時間帯）は常時稼働、夜間は停止」という構成を想定している。これは実際の小売現場の運用とも整合する現実的なコスト管理であり、本番運用を想定したコスト意識として明示する。

**Icebergのmetadata/manifest/data fileが際限なく増える問題にどう対応するか？**
Icebergはcheckpointのたびに新しいmetadata.jsonを追加する（上書きしない）仕様のため、放置すると本番運用ではファイル数が容易に数千〜数万に達する。この対策として`scripts/expire_snapshots.py`と`.github/workflows/iceberg_maintenance.yml`でExpire Snapshotsを実装している。ただし本ポートフォリオの実際の運用（検証セッションごとに`terraform destroy`でADLS2ごと環境を破棄する）では、蓄積は1セッション（数時間）分にしか発生せず、セッションをまたいで無限に増え続けるわけではない。にもかかわらず実装したのは、本番運用でこの問題が実際に起きること・その対処法を理解していることを示すため。cronによる定期実行ではなくworkflow_dispatch（手動実行）にしているのも同じ理由で、常時稼働しないAKS/ADLS2に対してスケジュール実行を組んでも大半は対象が存在せず失敗するだけであり、実際のライフサイクルに即した設計判断である。

**ADLS2へのネットワーク経路とアクセス認証はどこまで本番相当か？**
ADLS2は`public_network_access_enabled = true`のまま、ファイアウォールを`default_action = "Deny"`にしてAKSのサブネット（サービスエンドポイント）と検証用の自宅IPだけを許可している。`false`にするとPrivate Endpoint経由以外が全て拒否され、AKS上のFlinkから届かなくなるため。本番ならPrivate Endpointで公開エンドポイントを完全に閉じるのが正しいが、検証スクリプトを手元PCから実行できる構成を優先し、定石を知った上で簡略化している。同様に認証もストレージアカウントキーを使っており、本番ならWorkload Identity+RBACでキーを持たない構成にする。この2点は核心の動作確認後の改善候補として残している。
