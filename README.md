# dbt Learning Project

BigQueryを使ったdbt学習プロジェクト。E-Commerceデータを使ったデータ変換パイプラインの実装。

## 📊 プロジェクト概要

このプロジェクトは、dbtを使用してデータウェアハウス上でのデータ変換を実装し、分析可能なデータマートを構築します。

### 主な成果物

- **Stagingモデル**: 生データの基本的な整形と型変換
- **Martsモデル**: ビジネス分析用の集計テーブル
  - ユーザー別注文サマリー
  - 商品別売上サマリー

## 🛠 技術スタック

- **dbt-core**: 1.11.0
- **dbt-bigquery**: 1.10.2
- **BigQuery**: データウェアハウス
- **Python**: 3.11
- **Git/GitHub**: バージョン管理

## 📂 プロジェクト構造
```
dbt_learning/
├── models/
│   ├── staging/          # 元データの整形
│   │   ├── sources.yml   # ソース定義
│   │   ├── schema.yml    # テスト定義
│   │   ├── stg_orders.sql
│   │   ├── stg_users.sql
│   │   └── stg_products.sql
│   └── marts/            # 分析用テーブル
│       ├── schema.yml    # テスト定義
│       ├── user_orders_summary.sql
│       └── product_sales_summary.sql
├── dbt_project.yml
└── README.md
```

## 📈 データリネージ
```
raw_thelook (BigQuery)
    ├── orders
    ├── users
    └── products
         ↓
    {{ source() }}
         ↓
staging/
    ├── stg_orders
    ├── stg_users
    └── stg_products
         ↓
    {{ ref() }}
         ↓
marts/
    ├── user_orders_summary
    └── product_sales_summary
```

## ✅ テストカバレッジ

- **総テスト数**: 37個
- **成功率**: 97% (36 PASS, 1 WARN)
- **テスト内容**:
  - Unique制約チェック
  - Not Null制約チェック
  - データ型検証
  - 正の値チェック

## 🚀 セットアップ手順

### 1. リポジトリのクローン
```bash
git clone https://github.com/sato1046/dbt-learning-project.git
cd dbt-learning-project
```

### 2. dbtのインストール
```bash
pip install dbt-bigquery --break-system-packages
```

### 3. BigQuery認証設定

1. GCPプロジェクトでサービスアカウントを作成
2. JSONキーをダウンロード
3. `~/.dbt/profiles.yml`を設定
```yaml
dbt_learning:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /path/to/keyfile.json
      project: your-project-id
      dataset: dbt_learning
      location: asia-northeast1
      threads: 1
```

### 4. dbt実行
```bash
# モデルの実行
dbt run

# テストの実行
dbt test

# ドキュメント生成
dbt docs generate
dbt docs serve
```

## 📝 学習ログ

### Day 1（2時間）
- ✅ 環境構築完了
- ✅ Stagingモデル4つ作成
- ✅ Martsモデル1つ作成
- ✅ テスト12個実装
- ✅ ドキュメント生成

### Day 2（2時間）
- ✅ Martsモデル1つ追加（product_sales_summary）
- ✅ テスト拡張（37個に増加）
- ✅ README作成

## 🎯 今後の予定

- [ ] Intermediate層の追加
- [ ] Incremental modelの実装
- [ ] dbt packagesの導入
- [ ] CI/CDパイプラインの構築

## 📚 参考資料

- [dbt公式ドキュメント](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

## 👤 作成者

[@sato1046](https://github.com/sato1046)

## 📄 ライセンス

MIT License