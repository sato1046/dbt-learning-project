# 計測設計の意思決定ログ

最終更新：2026-06-12 21:58:59 +09:00
プロジェクト：dbt_learning_snowflake（4モデル）
目的：有料技術同人誌向け、dbt Core vs dbt Fusion の Snowflake 実測

---

## 決定1：キャッシュポリシー → コールド＆ウォーム両方測定（選択肢C）

### 採用理由
- 有料本として頒布する以上、「冷えた状態でも速いのか」「定常運用ではどうか」の両方が読者の関心事
- 比較軸が増えることで記事の情報密度が上がる
- Fusionの売り（高速起動・高速parse）が活きるのは主にコールド時。コールド計測なしではFusionの真価が見えない

### 不採用にした選択肢と理由
- A（コールド統一）：実運用感が薄れる。冷えた状態の数字だけでは実務読者の判断材料として不足
- B（ウォーム統一）：Fusionの強みであるコールド起動の速さが見えない。差が縮んで見え、Fusionの価値を過小評価する記事になる恐れ

---

## 決定2：Snowflake結果キャッシュ → 常に有効のまま（選択肢b）

### 採用理由
- 実運用シナリオに最も近い計測条件
- 本プロジェクトの4モデルは view 2本 + table 2本構成。CREATE VIEW / CREATE TABLE AS SELECT が走るため、そもそも結果キャッシュの効果は限定的
- 設定をシンプルに保つことで再現性が上がる（読者が同じ条件を作りやすい）
- USE_CACHED_RESULT = FALSE を差し込むと素の性能は測れるが、実運用とは乖離する

### 不採用にした選択肢と理由
- (a) コールド時は無効化：人工的な条件になり、実運用の参考値にならない
- (c) コールドは無効・ウォームは有効：理論的には実運用に最も近いが、CREATE系SQLが中心のため結果キャッシュの効果が薄く、複雑性に見合わない

### 注意点（本に書くべき）
- 結果キャッシュを有効のまま計測したことは明示する
- 4モデルの構成上、結果キャッシュの影響は限定的である旨も明記

---

## 決定3：計測順序 → Core/Fusion 交互（選択肢β）

### 採用理由
- 時間帯による外部要因（Snowflake側の負荷変動、ネットワーク状況）を Core/Fusion 両方に均等に分散できる
- Core を全部測ったあと Fusion を測ったら、たまたま Fusion の時間帯だけ Snowflake が空いていたという交絡を避けられる
- 統計的に妥当な比較を担保するための標準的な手法

### 不採用にした選択肢と理由
- (α) Core を5回まとめて → Fusion を5回まとめて：シンプルだが、時間帯の偏りが直接バイアスになる。有料本のデータとしては弱い

---

## 決定4：compile も計測対象に追加するか → ユーザー判断待ち

### 採用候補とする理由
- dbt parse はDAG構築までで軽量。Fusionの真価（Rust製パーサ、ローカル型チェック）が出やすいのは compile（Jinjaレンダリング + SQL生成）
- 本のネタとしてparseだけでなくcompileでも速いと示せると説得力が増す

---

## Snowflakeウェアハウス情報（2026-06-12 確認）

- ウェアハウス名：COMPUTE_WH
- サイズ：X-Small
- 種類：STANDARD
- auto_suspend：60秒
- 確認日時の状態：SUSPENDED

### 計測への影響
- 計測と計測の間に60秒以上の無アクティビティがあると自動でSUSPENDされる
- コールド状態を作るには、明示SUSPEND または 70秒以上の待機が必要
- ウォーム状態を保つには、60秒以内に次の計測を実行する必要がある

---

## 計測対象コマンド（暫定）

1. dbt parse  ：5回 × Core/Fusion × コールド/ウォーム = 20回
2. dbt compile：同上（採用なら）
3. dbt run   ：同上
4. dbt test  ：同上
5. dbt docs generate：1〜3回（生成物の比較がメインなので回数少なめでOK）

### コールド状態の作り方
- target/ ディレクトリ削除
- ウェアハウス SUSPEND（明示的に ALTER WAREHOUSE COMPUTE_WH SUSPEND、または70秒以上待機）

### ウォーム状態の作り方
- 1回 dummy run を実行してから計測開始（target/ も Snowflake側も温まった状態）
- 計測は60秒以内に次々と回す（auto_suspend を回避）

---

## 環境前提（00_environment.txt から要約）

- Windows 11 Pro 26100 / Intel Core Ultra 7 268V / RAM 31.52GB
- dbt Core 1.11.11（snowflake adapter 1.11.5）
- dbt Fusion 2.0.0-preview.183
- Python 3.12.0
- Snowflake account: ub90212.australia-southeast2.gcp（GCP Melbourne）
- 計測地：Sendai, JP（クロスリージョン）

---

## 決定4（確定）：compile も計測対象に追加 → YES

### 採用理由
- Fusionの売りであるRust製パーサ・ローカル型チェックは compile（Jinjaレンダリング + SQL生成）で真価が出る
- 記事の説得力強化：parseだけでなくcompileでも速いと示せる
- 工数追加が小さい：同じスクリプトで回せるため、コマンドを1つ増やすだけ
- Snowflakeコスト追加が小さい：compileは基本ローカル処理（ref/sourceの解決でメタデータ取得はあるが軽量）
- parse / compile / run / test / docs の5コマンドを揃えることで、Coreと完全な比較表が作れる

### 不採用案（parseのみ）の問題点
- Fusionの強みの一部しか見せられない
- 「parseは速いがcompileはどうなのか」という読者の疑問に答えられない

---

## 決定5：本計測前にリハーサル1回実施 → YES

### 採用理由
- 本計測は parse / compile / run / test / docs × Core/Fusion × コールド/ウォーム = 80回以上のコマンドを回す大規模計測
- スクリプトのバグが後半で発覚すると、全データが無駄になるリスクがある
- 有料本のデータとして再計測になると、執筆スケジュールに大きく影響する

### リハーサル内容
- Core で parse のみ、コールド5回 + ウォーム5回 = 10回を計測
- 所要時間：15〜20分程度
- 確認項目：
  1. Measure-Command で時間が正しく取れるか
  2. Tee-Object で生ログが文字化けせず保存されるか
  3. コールド/ウォーム切り替えが意図通り動くか（target/削除、ウェアハウスSUSPEND/RESUME）
  4. フォルダ構造・ファイル命名が後で集計しやすいか
  5. JST時刻がログに正しく記録されるか

### リハーサルで問題が見つかった場合
- スクリプトを修正してから本計測へ
- リハーサルのデータは本計測には使わず、参考データとして evidence/rehearsal/ に保存

### 不採用案（いきなり本計測）の問題点
- 80回以上回した後にバグ発覚 → 全データ無駄
- Snowflakeコストの無駄遣い
- 執筆スケジュールへの影響

---

## 決定6（リハーサルで発見）：PYTHONIOENCODING=utf-8 を必須設定とする

### 発見の経緯
- リハーサル1回目で、dbt Core が出力するファイルパスのうち日本語部分（「自主学習」）が文字化け
- 表示例：C:\Users\saori.sato\Documents\<<文字化け>>\dbt_learning_snowflake\target\perf_info.json
- 原因：Windows上のPythonがデフォルトで CP932(Shift-JIS) を使うため、UTF-8環境のPowerShellと不整合

### 対策
- セッションごとに次を設定：$env:PYTHONIOENCODING = "utf-8"
- これによりPythonの標準出力がUTF-8固定になり、Tee-Object でUTF-8保存したログでも文字化けしなくなる

### 効果検証
- 修正前ログ：Performance info: C:\...\<<文字化け>>\...
- 修正後ログ：Performance info: C:\...\自主学習\...
- 所要時間への影響：5.689秒 → 5.135秒（誤差範囲、性能への影響なし）

### 本に書くべき注意点（読者向け）
- 日本語パスを含むプロジェクトで dbt Core を使う場合、Windows + PowerShell では PYTHONIOENCODING=utf-8 の設定が必須
- 設定しないと、ログ・エラーメッセージ・パス情報が文字化けする
- dbt Fusion (Rust製) は元々UTF-8出力なのでこの問題は出ない可能性が高い（本計測で確認予定）
- [Console]::OutputEncoding = UTF8 だけでは不十分（Pythonが見ない）

### 環境設定の最終形（本計測前に毎セッション実行）

```powershell
Set-Alias dbtf "C:\Users\saori.sato\.local\bin\dbt.exe"
Remove-Item Env:\DBT_PROFILES_DIR -ErrorAction SilentlyContinue
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"
```

---

## 決定7〜10：本リハーサル完了時点での判断整理

### 決定7：テストデータの扱い → A（残す）

採用理由：
- 文字化け発見の証跡として有料本付録に使える
- 「リハーサルで何を発見したか」のストーリーが本に書ける
- CSV集計時は state カラムで簡単に除外できる

不採用案の理由：
- B（全消し）：証跡が意思決定ログだけになり、生のエビデンスとして弱い
- C（ログだけ残してCSVリセット）：集計のしやすさを優先するスタンス、有料本のリアル感は減る

---

### 決定8：外れ値の扱い → X3（両方併記）

採用理由：
- 「客観性を維持」「捏造禁止」の方針と完全に一致
- 「外れ値を含む値」「除外した値」両方提示することで読者が判断できる
- 「正直に書く」スタンスは技術書の信頼性に直結

不採用案の理由：
- X1（全データ使う）：外れ値の影響を隠すことになる
- X2（除外して平均）：除外の妥当性を読者が検証できない

---

### 決定9：バックアップ → A1（取らない）

採用理由：
- 本計測でrunが走れば、同じスキーマ・同じデータから再生成される（dbtの宣言的性質）
- スキーマも内容もSQLから一意に決まるので、保管する独自の価値がない
- 作業ステップを増やす理由がない

不採用案の理由：
- A2（CSVダウンロード）：再生成可能なものを別形式で保管する意味が薄い
- A3（別テーブルにコピー）：Snowflakeの容量を無駄に消費

---

### 決定10：計測順序の具体実装 → Y1（各回ごと完全交互）

採用理由：
- 時間バイアスを最も細かい粒度で均等化できる
- 各回ごとに target/削除 + 70秒待機 を挟むため、Core/Fusion 互いに完全コールドから始まる
- Y1''（target/リネーム保管）と実質的に同じ独立条件が得られ、複雑性は低い

不採用案の理由：
- Y1'（セット内交互）：時間効率は良いが、セット内のCore/Fusionが同じ時間帯になり外部要因バイアスが残る
- Y1''（target/分離保管）：厳密だが、Y1で十分独立条件が得られるため複雑性に対する追加メリットが小さい

コスト：
- コールド計測10回 = 75秒×10 ≒ 13分
- これを parse/compile/run/test で繰り返すと、コールドだけで約52分
- ウォーム計測込みで本計測全体は約1〜1時間20分

---

### 元データ確認結果（2026-06-12 22時台）

| テーブル | 種類 | 行数 | バイト | 最終更新 | データ期間 |
|---|---|---|---|---|---|
| RAW_CUSTOMERS | TABLE | 4 | 2048 | 2026-03-31 | - |
| RAW_SALES | TABLE | 1000 | 47104 | 2026-03-31 | 2018-01-03 〜 2023-06-24 |

3月の検証時から元データ無変化。本計測の前提が安定していることを確認。

既存の派生テーブル（参考、本計測で上書きされる）：
- FCT_MONTHLY_SALES（66行、2026-06-02 更新）
- FCT_SALES_SUMMARY（18行、2026-06-02 更新）
- MY_FIRST_DBT_MODEL（2行、テンプレ生成物の残骸）

---

### モデル依存関係（2026-06-12 確認）
sources.raw.raw_customers → stg_customers (view)

sources.raw.raw_sales     → stg_sales (view) → fct_monthly_sales (table)

                                    → fct_sales_summary (table)

注意：stg_customers は marts から参照されていない（4モデル中3モデルが依存関係に組み込まれている状態）。本に書くときは正確に明記する。

---

## 決定11：実務簡略版を本に含めるか → 含める（章として独立、削減は後判断）

採用理由：
- 「机上の空論ではなく実務に落とし込める内容」というご要望に直接応える要素
- ページ数（10ページ、Zenn 3〜4記事分）の範囲で削るかは後から判断できる
- 「削るのは後からでもできる」の方針通り、まず完成形を作る

実装方針：
- 本編：厳密版（コールド/ウォーム、Y1、5回計測、外れ値併記）
- 別章または付録：実務簡略版ガイド（3回ウォームのみ、Coreまとめ/Fusionまとめ、桁感判定）
- 簡略版の独立計測は不要。厳密版データから抜粋・再集計して提示する

不採用案：
- 含めない：実務読者へのフックが弱くなり、有料本の差別化要素が減る
- 考察内1〜2段落のみ：分量が薄すぎて「実務に落とし込める」の主張が形にならない

---

## 決定12：本計測の実施タイミング → 明日（2026-06-13）

採用理由：
- 23:10時点でこれから1時間20分走らせると深夜0時半完了
- 疲労時の長時間計測はトラブル対応の判断ミスを招きやすい
- 今日のうちに設計とスクリプトを完成させれば、明日は実行に集中できる

今日中に完了させるもの：
- 意思決定ログの確定（決定11、12含む）
- 本計測スクリプトの完成版（ファイル保存）
- 明日の再開手順のドキュメント化

---

## 明日（2026-06-13）の再開手順

PowerShellを開いた直後に以下を実行：

```powershell
# 1. エイリアス再設定（PowerShellは再起動でリセットされる）
Set-Alias dbtf "C:\Users\saori.sato\.local\bin\dbt.exe"

# 2. DBT_PROFILES_DIR 解除（BQ検証用に設定されていた場合の保険）
Remove-Item Env:\DBT_PROFILES_DIR -ErrorAction SilentlyContinue

# 3. UTF-8 設定（決定6で必須化）
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = "utf-8"

# 4. プロジェクトに移動
cd "C:\Users\saori.sato\Documents\自主学習\dbt_learning_snowflake"

# 5. 本計測用のエビデンスフォルダを作成（明日の日付）
$today = Get-Date -Format "yyyyMMdd"
$evidenceDir = ".\evidence\$today"
New-Item -ItemType Directory -Force -Path $evidenceDir | Out-Null

# 6. 本計測スクリプトを読み込んで実行
. .\evidence\20260612\main_measurement.ps1
```

注意：リハーサル（2026-06-12）と本計測（2026-06-13）はフォルダで分離する。本計測の結果は `evidence\20260613\` 配下に保存される。

---

## 決定13：test のコールド計測 → 含める（cold/warm両方計測）

### 採用理由
- Fusionの売りであるローカル型チェック・Rust製パーサは test のコールド時にこそ差が出やすい
- parse/compile/run/test の4コマンド全てで cold/warm 揃った比較表が、有料本の図表として一貫性と説得力を持つ
- 計測手順を「target/削除 → 70秒待機 → run（記録外）→ 即 test（記録対象）」と定義すれば、「Snowflake側がコールドな状態での test 計測」として概念的に一貫する
- この計測手順は本に明記すれば読者の誤解は生まれない
- 「客観性を維持」「捏造禁止」の方針上、test の cold データだけ落とすのは説明責任が重い

### 不採用案（test cold を計測対象から外し、warm 5回のみとする）の問題点
- test だけ warm のみという非対称な計測表は、本の図表として説明が増え読者を混乱させる
- Fusion の test 性能（cold 状態）が見えなくなる
- 「test cold は計測しなかった」と本に書くと、「なぜ?」という疑問が読者に残り、信頼性を損なう

### 計測手順の定義（本に書くべき）
- cold test 1回 = target/削除 + 70秒待機 + run（記録対象外）+ test（記録対象）
- 「test の cold 値」は「Snowflake側がコールドな状態から始まり、直前に run でテーブルが揃った状態での test 実行時間」と定義
- run（記録対象外）の所要時間は、cold run（記録対象）の別計測と参考比較可能

### コスト影響
- cold test 5回 × 2エンジン = 10回追加
- 各回所要：target/削除 + 70秒 + run（記録外、推定15〜30秒）+ test（推定10秒）= 約95〜110秒/回
- 追加時間：約16〜18分
- 全体見積：1時間20分 → 1時間40分前後

---

## 決定14：docs generate の計測 → warm 3回のみ（Core/Fusion 各3回 = 計6回）

### 採用理由
- docs generate は生成物（HTML/JSONカタログ）の比較がメインで、時間計測は副次的（「計測対象コマンド」セクションで「1〜3回でOK」と明記済み）
- cold/warm の性能差は parse/compile/run/test で十分取得できるため、docs まで両方測る必要性が薄い
- docs を cold/warm 各5回（計20回）に揃えると約20分の追加コストになるが、本の主張を強める効果は小さい
- warm 3回なら標準偏差を出すには最低限の試行数として機能する

### 不採用案（cold 3回 + warm 3回 = 12回）の問題点
- 約10分の追加時間に見合うインサイトが得られない
- docs だけ「cold/warm × 3回」と他コマンド（cold/warm × 5回）と試行数も状態も異なる扱いになり、本の構成が複雑化
- 生成物比較がメインという docs の位置付けと矛盾

### 注意点（本に書くべき）
- docs generate の時間計測は warm のみで実施したこと
- cold での挙動については「parse/compile/run/test の cold 値から推測される」旨を短評として扱う
- 生成物（catalog.json / manifest.json / index.html）の比較を主とし、時間は副次的指標

---

## 本計測の最終構成（決定13・14反映後）

| コマンド | エンジン | 状態 | 回数 | 小計 |
|---|---|---|---|---|
| parse | Core/Fusion | cold | 5 | 10 |
| parse | Core/Fusion | warm | 5 | 10 |
| compile | Core/Fusion | cold | 5 | 10 |
| compile | Core/Fusion | warm | 5 | 10 |
| run | Core/Fusion | cold | 5 | 10 |
| run | Core/Fusion | warm | 5 | 10 |
| test | Core/Fusion | cold | 5 | 10 |
| test | Core/Fusion | warm | 5 | 10 |
| docs generate | Core/Fusion | warm | 3 | 6 |
| **合計** | | | | **86回** |

- 計測順序：Y1（各回ごと完全交互、target/分離方式）
- target/ 分離：Core は `--target-path target_core`、Fusion は `--target-path target_fusion`
- run：DROP しない（CREATE OR REPLACE 任せ、本に明記）
- test cold：target/削除 → 70秒 → run（記録外）→ test（記録対象）
- 見積：約1時間40分
