# 実機確認結果（2026-06-26 作業実施、実行日2026-07-30）

※ 作業指示書の日付は2026-06-26だが、実際にコマンドを実行したのは2026-07-30。本計測（2026-06-13）から約1.5ヶ月後。この間隔自体が一部の結果（T5、T7）に影響している。

## サマリ

| タスク | 結論 | 記事への影響 |
|---|---|---|
| T1 環境確認 | Fusionは本計測時点と同じ preview.183 のまま。Coreは1.11.11で変化なし（1.12.0が利用可能だが未更新） | 記事の版数記述はそのままでよい |
| T2 コンパイル成果物 | ファイル数は8 vs 16だが、実体ノード数は両方4モデル+4テスト=8で一致。差分はFusionが追加出力する`.macro_spans.json`（診断用メタデータ、1ファイルにつき1つ）。**7.13×は同じノード集合の比較として成立する** | 「同じ仕事を比べている」と明記できる。ディレクトリ構造の違い（Fusionはtarget_fusion配下にgeneric_tests/フラット配置、Coreはyamlパスをミラー）も書ける |
| T3 並列度 | target_core/run_results.jsonは`dbt docs generate`の結果であり run/test の並列度検証には使えないと判明。アーカイブ済みコンソールログ（UTF-16LE）からCoreの実際のtest並列度=4、run並列度=2（DAG依存で制限）を確認。Fusionのtest並列度はJSONから4を直接確認。**Fusionのrun時の並列度は直接確認できなかった** | Core test/Fusion testはどちらも並列度4で「観測上は差が出ていない」と書ける。Fusion runの並列度は「未確認」と明記する必要がある |
| T4 manifestスキーマ | 両者とも `dbt_schema_version: v12.json` で完全一致。トップレベルキー差分なし（各17個） | 「v20を生成する」という公式記述と矛盾。「v12互換」の記述を支持。サイズ差（95KB）の原因はスキーマバージョンではなく未特定と書く |
| T5 --write-catalog | フラグは存在し、実行は成功（exit 0、catalog.json生成、index.htmlは生成されない）。**しかしSnowflakeの無料トライアルが期限切れでウェアハウスが停止しており、catalog.jsonの内容はnodes=0/sources=0の空**。フラグ自体の動作確認はできたが、実運用相当の内容検証はできなかった | 「フラグは機能する（起動・完了する）」とは書けるが、「実データが取れる」とは書けない。トライアル切れの事実も記事の環境注記に値する |
| T6 バージョンチェック無効化 | Fusionには `--no-version-check` / `DBT_DISABLE_VERSION_CHECK` が存在し確認できた。Coreの同名フラグ`--version-check`は無関係の別機能（require-dbt-versionのローカル比較）で、オンライン更新チェックを止める手段はCoreには見つからなかった | 再計測（T10）でこのフラグを使えば交絡を解消できると明記できる |
| T7 Tee-Objectエンコーディング | 公式ドキュメント（PS5.1版）は「常にUnicode(UTF-16LE)、-Encodingパラメータ自体が5.1には存在しない」と明記。既存ログ86本全てUTF-16LEで一致。**しかし今回(2026-07-30)同一マシン・同一PSバージョンで再テストしたところ、Tee-Objectの出力はUTF-8 BOMだった** — 公式記述・過去ログと矛盾する未解決の観測結果 | 「Windows PowerShell 5.1のTee-ObjectはUTF-16LE固定」という一般論は本計測時点では真だったが、恒久的事実として書くのは危険。時点情報を付けて書く必要がある |
| T8 終了コード取得 | `$exitCode = $LASTEXITCODE` はTee-Object/Out-Nullを挟んでも実行ファイル自身の終了コードを正しく捕捉する。ただし`*>&1`によるstderr統合はPowerShell側で`NativeCommandError`という別のエラーレコードを生成し、これはログ内容には現れるが`$LASTEXITCODE`には影響しない | 「exit_code=0は警告や不完全な結果がないことの証明にならない」という主張を、実際のdocs generate警告ログを根拠に具体的に書ける |

---

## T1：環境確認

### 実行したコマンド

```powershell
Set-Alias dbtf "C:\Users\saori.sato\.local\bin\dbt.exe" -Scope Global
$PSVersionTable.PSVersion
Get-Command dbt  | Format-List Name, CommandType, Source, Version
Get-Command dbtf | Format-List Name, CommandType, Definition
dbt  --version
dbtf --version
```

### 出力

```
Major  Minor  Build  Revision
-----  -----  -----  --------
5      1      26100  8875

Name        : dbt.exe
CommandType : Application
Source      : C:\Users\saori.sato\AppData\Local\Programs\Python\Python312\Scripts\dbt.exe
Version     : 0.0.0.0

Name        : dbtf
CommandType : Alias
Definition  : C:\Users\saori.sato\.local\bin\dbt.exe

Core:
  - installed: 1.11.11
  - latest:    1.12.0  - Update available!

  Your version of dbt-core is out of date!
  You can find instructions for upgrading here:
  https://docs.getdbt.com/docs/installation

Plugins:
  - bigquery:  1.11.1 - Update available!
  - snowflake: 1.11.5 - Update available!

dbt-fusion 2.0.0-preview.183
```

### 事実として言えること

- `dbtf` は `C:\Users\saori.sato\.local\bin\dbt.exe` に解決され、これは `dbt-fusion 2.0.0-preview.183` を返した。本計測時点のバージョンと**一致**しており、preview版の更新は行われていない。
- Core は 1.11.11 のままで変わっていない（1.12.0への更新が利用可能だが未実施）。
- PowerShellは5.1.26100.8875。
- **重要な注意**：新しいシェルでは `dbtf` エイリアスは失われていた（想定通り）。今回はPowerShellツールの制約上、cwd以外のシェル状態（変数・エイリアス）が呼び出しごとにリセットされるため、`dbtf`を使う全コマンドで`Set-Alias`を同一呼び出し内に含める必要があった。

### 未確認・不明な点

なし。

---

## T2：Core と Fusion のコンパイル成果物を比較

### 実行したコマンド

```powershell
"core:   " + (Get-ChildItem target_core\compiled   -Recurse -File | Measure-Object).Count
"fusion: " + (Get-ChildItem target_fusion\compiled -Recurse -File | Measure-Object).Count

Get-ChildItem target_core\compiled -Recurse -File |
  ForEach-Object { $_.FullName.Replace((Resolve-Path target_core\compiled).Path, '') } | Sort-Object

Get-ChildItem target_fusion\compiled -Recurse -File |
  ForEach-Object { $_.FullName.Replace((Resolve-Path target_fusion\compiled).Path, '') } | Sort-Object
```

### 出力

```
core:   8
fusion: 16
```

core側の相対パス一覧：
```
\dbt_learning_snowflake\models\marts\fct_monthly_sales.sql
\dbt_learning_snowflake\models\marts\fct_sales_summary.sql
\dbt_learning_snowflake\models\staging\sources.yml\source_not_null_raw_raw_customers_customer_id.sql
\dbt_learning_snowflake\models\staging\sources.yml\source_not_null_raw_raw_sales_transaction_id.sql
\dbt_learning_snowflake\models\staging\sources.yml\source_unique_raw_raw_customers_customer_id.sql
\dbt_learning_snowflake\models\staging\sources.yml\source_unique_raw_raw_sales_transaction_id.sql
\dbt_learning_snowflake\models\staging\stg_customers.sql
\dbt_learning_snowflake\models\staging\stg_sales.sql
```

fusion側の相対パス一覧：
```
\dbt_learning_snowflake\models\marts\fct_monthly_sales.macro_spans.json
\dbt_learning_snowflake\models\marts\fct_monthly_sales.sql
\dbt_learning_snowflake\models\marts\fct_sales_summary.macro_spans.json
\dbt_learning_snowflake\models\marts\fct_sales_summary.sql
\dbt_learning_snowflake\models\staging\stg_customers.macro_spans.json
\dbt_learning_snowflake\models\staging\stg_customers.sql
\dbt_learning_snowflake\models\staging\stg_sales.macro_spans.json
\dbt_learning_snowflake\models\staging\stg_sales.sql
\dbt_learning_snowflake\target_fusion\generic_tests\source_not_null_raw_raw_customers_customer_id.macro_spans.json
\dbt_learning_snowflake\target_fusion\generic_tests\source_not_null_raw_raw_customers_customer_id.sql
\dbt_learning_snowflake\target_fusion\generic_tests\source_not_null_raw_raw_sales_transaction_id.macro_spans.json
\dbt_learning_snowflake\target_fusion\generic_tests\source_not_null_raw_raw_sales_transaction_id.sql
\dbt_learning_snowflake\target_fusion\generic_tests\source_unique_raw_raw_customers_customer_id.macro_spans.json
\dbt_learning_snowflake\target_fusion\generic_tests\source_unique_raw_raw_customers_customer_id.sql
\dbt_learning_snowflake\target_fusion\generic_tests\source_unique_raw_raw_sales_transaction_id.macro_spans.json
\dbt_learning_snowflake\target_fusion\generic_tests\source_unique_raw_raw_sales_transaction_id.sql
```

### 事実として言えること

- Core は `.sql` ファイル8個（モデル4 + ジェネリックテスト4）。
- Fusion も `.sql` ファイルは8個（モデル4 + ジェネリックテスト4）で**Core と同数**。追加の8個は全て `.macro_spans.json`（コンパイル済みSQLファイル1つにつき1つ生成される、マクロ展開範囲のメタデータファイル）であり、コンパイル対象ノードそのものではない。
- したがって、Fusionのログが報告する `Processed: 4 models | 4 tests / Summary: 8 total` は、Coreが実際に生成した8個の `.sql` ファイル（4モデル+4テスト）と一致する。
- ディレクトリ構造の違い：
  - Core はジェネリックテストのコンパイル済みSQLを `models\staging\sources.yml\` 以下（ソースYAMLファイルのパスをミラーした階層）に置く。
  - Fusion はジェネリックテストを `target_fusion\generic_tests\` というフラットな専用ディレクトリに置き、YAMLパスをミラーしない。

### 判断

**Yes**。両エンジンは同じノード集合（4モデル + 4ジェネリックテスト = 8ノード）をコンパイルしている。ファイル数の差（8 vs 16）はコンパイル対象ノードの違いではなく、Fusionが追加で出力する診断用メタデータファイル（`.macro_spans.json`）による。**7.13×という比較は、比較対象のノード集合という観点では成立している。**

### 未確認・不明な点

`.macro_spans.json` の内容そのもの（マクロ展開範囲の詳細）は確認していない。記事に必要な範囲を超えるため深追いしなかった。

---

## T3：run_results.json から実際の並列度を確認

### 実行したコマンド

```powershell
$core = Get-Content target_core\run_results.json -Encoding UTF8 -Raw | ConvertFrom-Json
$fus  = Get-Content target_fusion\run_results.json -Encoding UTF8 -Raw | ConvertFrom-Json
# ノードごとの実行時刻、metadata、argsを確認
```

### 出力（要旨）

**target_core\run_results.json の `args`**：
```
"which": "generate",
"invocation_command": "dbt docs generate --target-path target_core",
"compile": true,
"target_path": "target_core"
```
→ このファイルは **`dbt docs generate` の結果**であり、`run`/`test` の結果ではなかった。全ノードの `execute` timing が `started_at == completed_at`、`execution_time` は0.01〜0.02秒程度で、実際のウェアハウス処理を伴わない値だった。

**target_fusion\run_results.json の `args`**：
```json
{"command": "test", "which": "test", "__other__": {"command": "test", "static_analysis": null, "which": "test"}}
```
→ このファイルは実際の **`test` コマンド**の結果。4ノード全てのスレッド情報とミリ秒精度タイムスタンプが取得できた：

| node | thread | exec(s) | started | completed |
|---|---|---|---|---|
| source_not_null...customers | Thread-4 | 1.1995503 | 20.528501300Z | 21.728051600Z |
| source_not_null...sales | Thread-3 | 0.9304737 | 20.528515300Z | 21.458989Z |
| source_unique...customers | Thread-2 | 0.9137901 | 20.530024500Z | 21.443814600Z |
| source_unique...sales | Thread-1 | 1.198903 | 20.530045Z | 21.728948Z |

4スレッド全てが `20.530` 付近から `21.44`以降まで完全に重なっており、**観測された並列度 = 4**。

**アーカイブ済みコンソールログ（evidence\20260613\logs、UTF-16LEで正しく読み直した）からの追加証跡**：

`core_test_cold_iter1.log`：
```
13:20:46  Concurrency: 4 threads (target='dev')
13:20:47  1 of 4 START test source_not_null_raw_raw_customers_customer_id ... [RUN]
13:20:47  2 of 4 START test source_not_null_raw_raw_sales_transaction_id ... [RUN]
13:20:47  3 of 4 START test source_unique_raw_raw_customers_customer_id ... [RUN]
13:20:47  4 of 4 START test source_unique_raw_raw_sales_transaction_id ... [RUN]
13:20:48  (4件とも同一秒内にPASS)
```
→ Core側のtestも同一秒内に4件全てSTART/PASSしており、**観測された並列度 = 4**（設定threads=4と一致）。

`core_run_cold_iter1.log`（モデルの依存関係あり）：
```
13:06:05  Concurrency: 4 threads (target='dev')
13:06:08  1 of 4 START sql view model stg_customers ... [RUN]
13:06:08  2 of 4 START sql view model stg_sales ... [RUN]
13:06:08  1 of 4 OK created ... [SUCCESS 1 in 0.91s]
13:06:08  2 of 4 OK created ... [SUCCESS 1 in 0.91s]
13:06:08  3 of 4 START sql table model fct_monthly_sales ... [RUN]
13:06:08  4 of 4 START sql table model fct_sales_summary ... [RUN]
13:06:13  4 of 4 OK created ... [SUCCESS 18 in 4.21s]
13:06:13  3 of 4 OK created ... [SUCCESS 66 in 4.24s]
```
→ ステージングモデル2つが先に並列実行・完了し、その後にファクトモデル2つが並列実行。**最大同時実行数 = 2**（依存関係グラフによる制限。4スレッド設定でも2ノードしか同時にreadyにならない）。

`fusion_run_cold_iter1.log`：
```
 Succeeded [  0.82s] model stg_customers (view)
 Succeeded [  1.13s] model stg_sales (view)
 Succeeded [  1.27s] model fct_monthly_sales (table)
 Succeeded [  1.28s] model fct_sales_summary (table)
Finished 'run' successfully for target 'dev' [4.3s]
```
→ Fusionのrunコンソールログにはノードごとの開始/終了タイムスタンプやスレッドIDが出力されない。個別所要時間の合計（約4.5秒）と総所要時間（4.3秒）が近いことから並行度が低い可能性を示唆するが、これは推測であり直接観測ではない。

### 事実として言えること

- target_core\run_results.json は `docs generate` の結果であり、当初想定していた「run/testの並列度確認」には使えなかった。
- target_fusion\run_results.json（`test`コマンド）からは、4テストノードが完全に重なって実行された（観測並列度=4）ことを直接確認できた。
- アーカイブ済みコンソールログから、Coreの `test` は観測並列度4、Coreの `run`（依存関係あり）は観測最大並列度2（DAGによる制限）であることを直接確認できた。
- Fusionの `run` コマンドでの実際の並列度は、今回入手できた成果物（JSON・コンソールログとも）からは**直接確認できなかった**。

### 未確認・不明な点

- Fusionの `run`（モデル依存関係あり）の実際の並列度。target_fusion\run_results.json は最後に実行された `test` コマンドで上書きされており、`run` 時点のスナップショットは残っていない。コンソールログにも thread_id・タイムスタンプが出力されない。
- 「Snowflakeでは threads 設定を無視して自動最適化する」という公式記述を、この4ノードの独立テストという小さいケースだけで確認・反証することはできない（Core側もthreads=4設定であり、4件の独立ノードなら同じ並列度4になるのは自然な結果のため、この観測だけでは公式記述の裏付けにも反証にもならない）。

---

## T4：manifest.json のスキーマバージョン

### 実行したコマンド

```powershell
$c = Get-Content target_core\manifest.json   -Encoding UTF8 -Raw | ConvertFrom-Json
$f = Get-Content target_fusion\manifest.json -Encoding UTF8 -Raw | ConvertFrom-Json
$c.metadata | ConvertTo-Json -Depth 3
$f.metadata | ConvertTo-Json -Depth 3
```

### 出力

Core `metadata`：
```json
{
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
    "dbt_version": "1.11.11",
    "project_name": "dbt_learning_snowflake",
    "user_id": "89407c76-2e4e-4d2f-abf4-3a2fe5d89bff",
    "send_anonymous_usage_stats": true,
    "adapter_type": "snowflake",
    "quoting": {"database": false, "schema": false, "identifier": false, "column": null}
}
```

Fusion `metadata`：
```json
{
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
    "dbt_version": "2.0.0-preview.183",
    "project_name": "dbt_learning_snowflake",
    "user_id": null,
    "send_anonymous_usage_stats": null,
    "adapter_type": "snowflake",
    "quoting": {"database": false, "identifier": false, "schema": false}
}
```

トップレベルキー差分：
```
core only:
fusion only:
core keys count: 17  fusion keys count: 17
```
（差分なし、両者とも17キー）

### 事実として言えること

- 両者とも `dbt_schema_version` は **`https://schemas.getdbt.com/dbt/manifest/v12.json`** で完全一致。
- トップレベルキーの数・内容に差分はない（各17個）。
- 細かい差：Core の `quoting` オブジェクトには `column: null` キーがあるが、Fusion側の `quoting` オブジェクトには `column` キー自体が存在しない。
- Fusion側は `user_id` と `send_anonymous_usage_stats` が `null`。

### 事実として言えること（記事への反映）

公式ドキュメント2件のうち、「Fusion は Core と互換の v12 manifest を生成する」（Upgrading to v2.0）が実機と一致し、「Fusion は v20 manifest を生成する」（Upgrade versions in dbt platform）は実機と**一致しなかった**。

manifest.jsonのサイズ差（Core 627,417B / Fusion 532,011B、約15%小さい）は、スキーマバージョンの違いによるものではない。原因は未特定（トップレベルキー構造は同一のため、ノードやマクロなど内部コンテンツの詳細差分が原因と推測されるが、これは指示範囲を超えるため深追いしていない）。

### 未確認・不明な点

サイズ差の直接の原因（未特定、深追いしない方針で確認）。

---

## T5：`dbt compile --write-catalog` が catalog.json を生成するか

### 実行したコマンド

```powershell
dbtf compile --help | Select-String -Pattern "write-catalog" -Context 0,2

dbtf compile --write-catalog --target-path target_fusion_catalogtest 2>&1 |
  Out-File -Encoding UTF8 evidence\20260626\T5_fusion_write_catalog.log
"LASTEXITCODE = $LASTEXITCODE"

Get-ChildItem target_fusion_catalogtest | Select-Object Name, Length, LastWriteTime
Test-Path target_fusion_catalogtest\catalog.json
Test-Path target_fusion_catalogtest\index.html
```

### 出力

ステップ1（フラグの存在確認）：
```
--write-catalog
    Write a catalog.json file to the target directory
    [env: DBT_WRITE_CATALOG=]
```
→ フラグは存在した。

ステップ2（実行ログ全文、`evidence\20260626\T5_fusion_write_catalog.log`）：
```
dbt-fusion 2.0.0-preview.183
   Loading ~/.dbt/profiles.yml
Preparing to write catalog.json
[Non-critical] Issue fetching catalog for schema 'dbt_learning.dbt_learning': [Snowflake] 390913 (08004): Your free trial has ended and all of your virtual warehouses have been suspended. Add billing information in the Snowflake web UI to continue using the full set of Snowflake features. [466e5967-1f87-42f1-8362-58cec8580b2a]
(in :2:29)
Fetched full catalog.json results
Encountered some issues building catalog.json, these did not affect job execution
Successfully wrote catalog.json
New version available 2.0.0-preview.203 (run `dbt system update`)

==================== Execution Summary =====================
Finished 'compile' successfully for target 'dev' [7.9s]
Processed: 4 models | 4 tests
Summary: 8 total | 8 success

LASTEXITCODE = 0
```

ステップ3（生成物）：
```
Name                    Length LastWriteTime
----                    ------ -------------
compiled                       2026/07/30 21:11:47
generic_tests                  2026/07/30 21:11:39
snapshots                      2026/07/30 21:11:39
catalog.json               608 2026/07/30 21:11:47
manifest.json           532200 2026/07/30 21:11:47
run_results.json           389 2026/07/30 21:11:47
semantic_manifest.json     234 2026/07/30 21:11:39

Test-Path catalog.json -> True
Test-Path index.html   -> False
```

catalog.json の中身（全文）：
```json
{"metadata":{"dbt_schema_version":"https://schemas.getdbt.com/dbt/catalog/v1.json","dbt_version":"2.0.0-preview.183","generated_at":"2026-07-30T12:11:47.266642100Z","invocation_id":"019fb2ef-fab0-7a71-8c90-fcc41c59ac98","env":{}},"nodes":{},"sources":{},"errors":["[Non-critical] Issue fetching catalog for schema 'dbt_learning.dbt_learning': [Snowflake] 390913 (08004): Your free trial has ended and all of your virtual warehouses have been suspended. Add billing information in the Snowflake web UI to continue using the full set of Snowflake features. [466e5967-1f87-42f1-8362-58cec8580b2a]\n(in :2:29)"]}
```
→ nodes count = 0、sources count = 0。

### 事実として言えること

- `--write-catalog` フラグは preview.183 に存在し、`dbtf compile --write-catalog --target-path <新規ディレクトリ>` は **exit code 0 で成功**する。
- `catalog.json` は生成されるが、`index.html` は生成されない（Fusionのdocs generate警告メッセージが案内する通り、index.htmlはdbt Core側のものを流用する設計）。
- **重大な環境変化**：Snowflakeの無料トライアルが期限切れになっており、全ウェアハウスが停止していた（`Your free trial has ended and all of your virtual warehouses have been suspended`）。これは本計測（2026-06-13）時点では発生していなかった問題であり、今回（2026-07-30）初めて確認された。
- このため、生成された catalog.json はスキーマ的には正しい（`dbt_schema_version: v1.json`）が、`nodes` と `sources` が両方とも空（0件）だった。カラムメタデータ取得にはウェアハウスの起動が必要なため。
- Core側の catalog.json（本計測時点、7,869B、実データ入り）と今回のFusion catalog.json（608B、空データ）は、内容の充実度が全く異なるが、これは**Fusionの制約ではなく、Snowflakeトライアル期限切れという環境要因**によるもの。

### 未確認・不明な点

- ウェアハウスが正常稼働している状態で `--write-catalog` が実データを含む完全なcatalog.jsonを生成できるかどうかは、**今回は確認できなかった**。billing情報を追加してトライアルを再開しない限り再検証不可。
- この環境変化は、本作業のT10（再計測）だけでなく、`run`/`test`/`build`など warehouse を必要とする今後のあらゆる計測に影響する。billing対応はSaoriさんの判断事項のため、ここでは対応していない。

---

## T6：Fusion のバージョンチェックを無効化する手段があるか

### 実行したコマンド

```powershell
dbtf --help | Select-String -Pattern "version|update|check|offline|telemetry"
dbtf parse --help | Select-String -Pattern "version|update|check|offline"
dbtf system --help
dbt --help | Select-String -Pattern "version-check|no-version-check"
Get-ChildItem Env: | Where-Object { $_.Name -like "DBT*" } | Format-Table -AutoSize
```

### 出力

`dbtf parse --help`（`compile --help`、`system --help` にも同一オプションが共通で表示された）：
```
--no-version-check
    Disable online version check for dbt-fusion updates
    [env: DBT_DISABLE_VERSION_CHECK=]
```

`dbtf --help`（トップレベル、サブコマンド一覧のみ）から見つかったボーナス情報：
```
docs   Generate and serve documentation (deprecated in Fusion - use `dbt compile --write-catalog`)
```
→ Fusion公式ヘルプが `docs generate` の非推奨と `compile --write-catalog` への移行を明記している（T5の前提を裏付ける一次情報）。

`dbt --help`（Core）：
```
--version-check / --no-version-check
    If set, ensure the installed dbt version matches the require-dbt-version
    specified in the dbt_project.yml file (if any). Otherwise, allow them to differ.
```

`Get-ChildItem Env:DBT*`：出力なし（DBT関連の環境変数は現在のシェルには設定されていない）。

### 事実として言えること

- Fusionには **`--no-version-check`（環境変数 `DBT_DISABLE_VERSION_CHECK`）** が存在し、「dbt-fusionの更新に関するオンラインバージョンチェックを無効化する」ことが明記されている。ログに出ていた `New version available X.X.X` メッセージを止める手段として使える。
- Coreにも同名に見える `--version-check`/`--no-version-check` が存在するが、**意味が全く異なる**。これは `dbt_project.yml` の `require-dbt-version` とのローカルな整合性チェックであり、オンラインの更新チェックとは無関係。Coreにはオンライン更新通知を止める専用フラグは見つからなかった（そもそもCoreのログにはこの種の通知が出ていない）。
- DBT関連の環境変数は現在未設定。

### 未確認・不明な点

Coreが実際に裏でオンライン更新チェック相当の通信をしているか（表示していないだけの可能性）は、ヘルプの探索だけでは確認できない。ネットワーク監視などが必要になり、今回の指示範囲を超えるため実施していない。

---

## T7：`Tee-Object` の既定エンコーディングを確認

### 実行したコマンド

```powershell
New-Item -ItemType Directory -Path evidence\20260626\scratch -Force
"encoding test あいうえお" | Tee-Object -FilePath evidence\20260626\scratch\tee_default.txt | Out-Null
"encoding test あいうえお" | Out-File -Encoding UTF8 evidence\20260626\scratch\outfile_utf8.txt
Format-Hex evidence\20260626\scratch\tee_default.txt  | Select-Object -First 2
Format-Hex evidence\20260626\scratch\outfile_utf8.txt | Select-Object -First 2

Get-ChildItem evidence\20260613\logs\*.log | ForEach-Object {
  $b = [System.IO.File]::ReadAllBytes($_.FullName)[0..2]
  ...
} | Group-Object Encoding | Select-Object Name, Count
```

### 出力

`tee_default.txt`（Tee-Object、-Encoding未指定）の先頭バイト：
```
00000000   EF BB BF 65 6E 63 6F 64 69 6E 67 20 74 65 73 74  ï»¿encoding test
```
→ **EF BB BF = UTF-8 BOM**。

`outfile_utf8.txt`（Out-File -Encoding UTF8）の先頭バイト：
```
00000000   EF BB BF 65 6E 63 6F 64 69 6E 67 20 74 65 73 74  ï»¿encoding test
```
→ 同じくUTF-8 BOM（想定通り）。

既存ログ86本のエンコーディング内訳：
```
Name     Count
----     -----
UTF-16LE    86
```
→ 86本全てUTF-16LE（先頭バイト FF FE）で一致。

`$PSVersionTable.PSVersion`：`5.1.26100.8875`（T1と同じ、今回のテストもT1と全く同じPowerShellバージョン上で実施）。

Microsoft Learn `Tee-Object`（PowerShell 5.1版、`https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.utility/tee-object?view=powershell-5.1`）の記載：
> `Tee-Object` uses "Unicode" (UTF-16LE) encoding when it writes to files. If you need a different encoding, use the `Out-File` cmdlet with the **Encoding** parameter.

また、PowerShell 5.1版の `Tee-Object` の構文には `-Encoding` パラメータ自体が存在しない（`-Encoding` は7.2以降で追加されたとの記載が7.6版ドキュメントにある）。つまり公式記述上は、Windows PowerShell 5.1では **Tee-Objectは常にUTF-16LEで書き込まれ、上書きする手段がない** ことになっている。

### 事実として言えること

- Microsoft公式ドキュメント（PS5.1スコープ）は「Tee-ObjectはUnicode(UTF-16LE)固定」と明記している。
- 2026-06-13の計測で生成された86本のログは、実際に全てUTF-16LEだった。これは公式記述と一致する。
- **しかし、2026-07-30に同一マシン・同一PowerShellバージョン（5.1.26100.8875）で `Tee-Object -FilePath`（-Encoding未指定）を実行したところ、出力はUTF-8 BOMだった。** これは公式ドキュメントの記述および過去のログの実測結果と**矛盾する**。
- 計測スクリプト（`01_functions.ps1` 52行目・54行目）を確認したが、`Tee-Object` の呼び出し方自体は今回のテストと同一（`-Encoding` は指定していない）。したがってこの差はスクリプトの書き方の違いによるものではない。

### 未確認・不明な点

- なぜ同一マシン・同一PowerShellバージョンで挙動が変わったのかは**特定できなかった**。Windows Updateによるパッチ、システムロケールの「ベータ: ワールドワイド言語サポートにUnicode UTF-8を使用する」設定の変更、PowerShellプロファイルの変更などが考えられるが、いずれも検証していない（指示書に「深追いしない」とあるT4の方針に準じ、原因調査までは実施していない）。
- したがって、記事には「Windows PowerShell 5.1のTee-Objectは常にUTF-16LEで書き込まれる」という**時制のない一般論として書くのは不適切**。「2026-06-13の計測環境ではUTF-16LEだった（公式ドキュメントの記述通り）。ただし2026-07-30の再検証では同一マシン・同一バージョンでUTF-8 BOMとなり、この挙動が環境によって変わりうることが分かった」という時点を明記した書き方が正確。

---

## T8：計測スクリプトの終了コード取得箇所抽出

### 実行したコマンド

```powershell
Get-Content evidence\20260612\01_functions.ps1 -Encoding UTF8
```

### 出力（該当箇所、`01_functions.ps1` 49〜58行目）

```powershell
    # 計測対象コマンド実行
    $startTime = Get-Date
    if ($Command -eq "docs generate") {
        & $cmdExe docs generate --target-path $TargetPath *>&1 | Tee-Object -FilePath $logFile | Out-Null
    } else {
        & $cmdExe $Command --target-path $TargetPath *>&1 | Tee-Object -FilePath $logFile | Out-Null
    }
    $exitCode = $LASTEXITCODE
    $endTime = Get-Date
```

補強証跡（`evidence\20260613\logs\fusion_docs_generate_warm_iter1.log`、UTF-16LEで正しく再デコード）：
```
dbt.exe :
発生場所 C:\Users\...\01_functions.ps1:52 文字:9
+         & $cmdExe docs generate --target-path $TargetPath *>&1 | Tee- ...
    + CategoryInfo          : NotSpecified: (:String) [], RemoteException
    + FullyQualifiedErrorId : NativeCommandError

=================== Errors and Warnings ====================
[warning] [DocsGenerateWarning (dbt1705)]: `dbt docs generate` is not supported. Use `dbt compile --write-catalog`
to write catalog.json. To host docs locally, use the dbt Core index.html with catalog.json and manifest.json
in the same directory: https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/task/docs/index.html

==================== Execution Summary =====================
Finished 'docs' with 1 warning [515ms]
```

対応するCSV行（`evidence\20260613\results.csv`、該当行を確認）は `exit_code=0` だった。

### 事実として言えること

- 終了コードの取得は `& $cmdExe ... *>&1 | Tee-Object -FilePath $logFile | Out-Null` を実行した**直後**に `$LASTEXITCODE` を読む、というシンプルな1行のみ。`try/catch` は使われていない。
- `*>&1` は標準出力・標準エラーなど全ストリームを1つにまとめてパイプに流す。この統合によって、`dbt-fusion` がstderr相当に書いた警告テキストは、PowerShell側で `NativeCommandError` という**別のエラーレコード**（`FullyQualifiedErrorId: NativeCommandError`）としてログに記録される。
- しかし、この `NativeCommandError` の発生は `$LASTEXITCODE` の値に影響しない。`$LASTEXITCODE` は純粋に、起動した実行ファイル（`dbtf`）自身が返したプロセス終了コードを反映する。
- 実例：`fusion_docs_generate_warm_iter1.log` では `NativeCommandError` が記録され、`[DocsGenerateWarning (dbt1705)]` という明確な警告（docs generateが非推奨であること）が出ているにもかかわらず、対応するCSV行の `exit_code` は `0` だった。dbt-fusion自身は「警告付きで正常終了（`Finished 'docs' with 1 warning`）」と判断してプロセスを exit(0) で終了させたため。

### 未確認・不明な点

なし。指示範囲内で明確に確認できた。

---

## T10：再計測の可否判定

- **技術的に可能か**：可能。`dbtf parse` / `dbtf compile` に `--no-version-check`（または環境変数 `DBT_DISABLE_VERSION_CHECK=1`）を追加した上で、既存の `Invoke-Measurement` 関数（`01_functions.ps1`）をそのまま再利用し、cold/warm × 5回程度を再実行すればよい。Core側は該当するオンライン更新チェックの仕組み自体が見つかっていないため、Core側の再計測は原則不要（対称性のために実施してもよいが必須ではない）。
- **実現方法**：
  ```powershell
  & dbtf parse --no-version-check --target-path <新規ディレクトリ> *>&1 | Tee-Object -FilePath $logFile | Out-Null
  ```
  のように、既存スクリプトの `dbtf` 呼び出し部分にオプションを1つ追加するだけで済む。**必ず新規の `--target-path` を使うこと**（既存のtarget_fusionを上書きしない）。
- **想定所要時間**：parse/compile × cold/warm(2状態) × 5回 = 20回の実行。T6で確認した個々の所要時間（0.7〜1.6秒程度）に加え、cold計測前の待機（`Wait-ColdState`、既定70秒）が cold 5回分で計350秒（約6分）かかる。合計で概ね10〜15分程度と見積もられる。Snowflakeへの接続を伴わない（parse/compileはウェアハウスを起動しない）ため、追加の課金は基本的に発生しないと考えられる。
- **T5で判明した注意点**：Snowflakeの無料トライアルが期限切れでウェアハウスが停止している。parse/compileはウェアハウスを使わないため今回のT5の実行自体は正常に完了したが、これは「parse/compileがトライアル期限切れの影響を受けない」ことの完全な保証ではない（今回確認した範囲での観測に基づく推測）。
- **本作業では実行していない**（指示書の指定通り、判定のみ）。

---

## 作業中に気づいたこと

1. **Snowflakeの無料トライアルが期限切れになっている**（T5で発見）。「Your free trial has ended and all of your virtual warehouses have been suspended」というエラーが出ており、ウェアハウスを必要とする操作（run/test/build、および--write-catalogでの実カラムメタデータ取得）は現状実行できない。本計測（2026-06-13）時点では問題なく動いていたため、この1.5ヶ月の間に発生した変化。billing対応をしない限り、今後の追加計測（T10含む）はwarehouseを使わない範囲（parse/compile）に限定される。
2. **target_core / target_fusion のJSON成果物は「最後に実行されたコマンド」の結果を反映する**ことをT3で再確認した。target_core\run_results.json は実は `dbt docs generate` の結果であり、`run`/`test` の結果ではなかった。今後この2つのディレクトリの成果物を記事の根拠として引用する際は、`args`/`invocation_command` を必ず確認し、想定しているコマンドの結果であるかを都度確認する必要がある。
3. Fusion公式ヘルプ（`dbt --help`）は `docs` サブコマンドの説明に「deprecated in Fusion - use `dbt compile --write-catalog`」と明記しており、さらに `docs generate` 実行時の警告メッセージには「dbt Coreのindex.htmlをFusionのcatalog.json・manifest.jsonと同じディレクトリに置いて使う」という具体的な回避策のリンク（dbt-labs/dbt-core GitHubリポジトリ）が示されていた。これは記事で「回避策」を書く際に一次情報として引用できる。
4. PowerShellツール（本作業の実行環境）はカレントディレクトリ以外のシェル状態（変数・エイリアス）を呼び出しごとに保持しないため、`dbtf` エイリアスは毎回のコマンド内で再設定する必要があった。これは検証作業自体の話であり、dbt/dbt Fusionの挙動とは無関係。

## 実行しなかった・できなかったタスクとその理由

- **T10の実行**：指示書で明示的に禁止されていたため実行していない（判定のみ）。
- **T5のcatalog.json実データ検証**：Snowflakeトライアル期限切れによりウェアハウスが起動できず、実カラムメタデータを含む完全なcatalog.jsonが生成されるかどうかは確認できなかった。
- **T7のTee-Objectエンコーディング差異の原因調査**：今回と過去計測時点での挙動の違いの原因（Windows Update、システムロケール設定変更など）は特定していない。指示書の「深追いしない」方針に従い、事実の記録のみとした。
- **T4のmanifest.json内部コンテンツの詳細差分解析**：指示書で明示的に対象外とされていたため実施していない（スキーマバージョンとトップレベルキー差分の確認のみ）。
- **T6のCore側オンライン通信の実態調査**：ヘルプに専用フラグが見つからなかった以上のことを確認するにはネットワーク監視等が必要であり、指示範囲を超えるため実施していない。
