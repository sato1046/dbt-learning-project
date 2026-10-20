# 本計測後の意思決定ログ

最終更新：2026-06-26（JST）
プロジェクト：dbt_learning_snowflake（4モデル）
前提：`evidence\20260612\01_decisions.md` の決定1〜14を継承

---

## 決定15：本計測完了の記録（実施ログ）

### 実施結果
- 計測日：2026-06-13(JST)
- 開始〜終了：21:38:57 〜 22:35:05（経過 56.1分）
- 総計測数：86回（決定14の最終構成通り）
- 失敗数：0回（全 exit_code=0）
- 環境：決定1〜14策定時から変更なし（dbt Core 1.11.11 / dbt Fusion 2.0.0-preview.183 / Snowflake GCP Melbourne）

### 採用された設計（決定1〜14のうち本計測で適用したもの）
- 決定1：cold/warm 両方測定
- 決定2：Snowflake結果キャッシュ常時有効
- 決定3：Core/Fusion 交互（実装は決定10のY1）
- 決定4：compile を計測対象に追加
- 決定6：PYTHONIOENCODING=utf-8 必須化
- 決定8：X3 外れ値含む/除外の併記
- 決定9：バックアップ取らない（再生成可能性を前提）
- 決定10：Y1（各回ごと完全交互、target/分離方式）
- 決定13：test cold は `run（記録外）+ test（記録対象）` 手順
- 決定14：docs generate は warm 3回のみ

### 結果格納場所
- `evidence\20260613\results.csv`（CSV正本、87行 = ヘッダ + 86計測）
- `evidence\20260613\logs\`（生ログ86本）
- `evidence\20260613\simple_report.md`（CSV正本との数値照合済み、2026-06-14に発見X反映で更新、2026-06-26に実機確認結果を反映）

### 計測中の所感
- 見積1h40m に対し実測56分（決定14時点の保守的見積より44分短縮）
- 全86回 exit_code=0、リハーサルで発見した文字化け（決定6）を含め本計測中の新規バグなし

---

## 決定16：Fusion docs generate 未サポートの扱い方針

### 観測された事実
- Fusion 2.0.0-preview.183 で `dbt docs generate` 実行時、3回とも以下の警告を出力して終了：

  ```
  [warning] [DocsGenerateWarning (dbt1705)]: `dbt docs generate` is not supported.
  Use `dbt compile --write-catalog` to write catalog.json.
  ```

- 所要時間：506ms / 515ms / 615ms（exit_code=0、警告終了）
- Core 側の同コマンドは catalog.json を生成して 10.172秒（平均、n=3）
- Fusion 自身が代替手段として `dbt compile --write-catalog` を案内

### 採用方針：Fusion docs generate の計測値を速度比較から除外、「機能制約」として独立扱い
- simple_report.md セクション2の速度比較表から docs generate 行を削除
- セクション4 発見1から「17倍速」記述を削除、最大差は compile cold（7.13×）に書き換え
- セクション4 に「発見X：Fusion preview 版の機能制約（dbt docs generate 未サポート）」を新規追加
- 本でも同様の構成（速度比較章ではなく機能制約章）で記載

### 採用理由
- 「Fusion 17倍速」と書くと、Fusion が実処理を高速に行ったと読者に伝わるが、観測事実は「警告を出して終了している」
- 一方で「Fusion preview 段階では Core で動くコマンドの一部が未サポート」という事実は、実務読者にとって有料本の重要な知見
- 観測事実のみに基づき、速度比較ではなく機能制約として扱うのが、執筆原則（捏造禁止・推測明示）と整合

### 不採用案と理由
- 案A：そのまま「Fusion 17倍速」として記載
  - 問題：読者への誤導。Fusion が実処理をしていない事実を隠蔽することになり、本の信頼性を毀損
- 案B：docs generate のデータを完全に削除し言及しない
  - 問題：preview 版の制約という実務上重要な知見を失う。読者が同じ環境で詰まる可能性

### 実機確認結果（2026-06-26）
- `target_fusion\` 配下のファイル一覧確認結果：
  - **catalog.json：存在せず**
  - **index.html：存在せず**
  - manifest.json：存在（532,011B、Core の 627,417B より約95KB小さい）
  - その他：`compiled/`、`generic_tests/`、`run/`、`snapshots/`、`run_results.json`、`semantic_manifest.json`
- 確認方法：PowerShell `Get-ChildItem` および `Test-Path` で `target_fusion\` 配下を列挙
- 確認時のファイル更新時刻：2026-06-13 22:31〜22:35 のまま（本計測以降に上書きされていない）
- **結論**：発見X「Fusion は docs generate 未サポート」の事実が決定的に裏付けられた
- **追加で判明した論点**（別タスク化）：manifest.json のサイズ差（Core 627,417B vs Fusion 532,011B、約95KB減、約15%減）。中身比較は未実施

### 影響範囲（反映済み）
- simple_report.md：セクション1（脚注追加）、セクション2（行削除＋注記）、セクション4 発見1（書き換え）、発見X（新規＋2026-06-26 実機確認結果追記）、セクション6（実機確認結果反映）
- 本ドラフト：未着手のため、構成段階で反映予定

---

## 残論点（別タスク化）

### manifest.json のサイズ差（約95KB減、約15%減）
- Core: 627,417B / Fusion: 532,011B
- 仮説段階：Fusion は Rust 実装のため Jinja マクロの内部表現が異なる可能性、または preview 版で一部メタ情報が未実装の可能性
- 検証方法案：両 manifest.json を JSON としてパースし、トップレベルキー・サイズ別の差分を確認
- 結論が出るまで本ドラフトには書かない
