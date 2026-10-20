# dbt Core vs dbt Fusion 本計測 簡易レポート

**計測日時**：2026-06-13 21:38:57 〜 22:35:05 (経過 56.1分)
**実施場所**：Sendai, JP（Snowflake: GCP Melbourne、クロスリージョン）
**総計測数**：86回（全てexit_code=0、失敗ゼロ）
**設計準拠**：意思決定ログ 決定1〜14（target/分離方式、Y1各回交互、X3外れ値併記）

---

## 1. 結果サマリ（決定8：X3 外れ値含む / 除外の併記）

| Command | State | Engine | n | 平均(秒) | SD | CV% | IQR除外後 平均(n) |
|---|---|---|--:|--:|--:|--:|---|
| parse | cold | core | 5 | **5.139** | 0.072 | 1.4% | 5.162 (n=3, 2件除外) |
| parse | cold | fusion | 5 | **1.284** | 0.502 | 39.1% | 外れ値なし |
| parse | warm | core | 5 | **4.202** | 0.071 | 1.7% | 4.175 (n=4, 1件除外) |
| parse | warm | fusion | 5 | **0.917** | 0.318 | 34.7% | 外れ値なし |
| compile | cold | core | 5 | **7.381** | 0.544 | 7.4% | 7.144 (n=4, 1件除外) |
| compile | cold | fusion | 5 | **1.035** | 0.179 | 17.3% | 外れ値なし |
| compile | warm | core | 5 | **5.779** | 0.329 | 5.7% | 外れ値なし |
| compile | warm | fusion | 5 | **0.931** | 0.160 | 17.1% | 0.867 (n=4, 1件除外) |
| run | cold | core | 5 | **12.784** | 1.216 | 9.5% | 12.271 (n=4, 1件除外) |
| run | cold | fusion | 5 | **5.079** | 0.503 | 9.9% | 外れ値なし |
| run | warm | core | 5 | **9.770** | 0.295 | 3.0% | 外れ値なし |
| run | warm | fusion | 5 | **3.576** | 0.226 | 6.3% | 3.478 (n=4, 1件除外) |
| test | cold | core | 5 | **7.344** | 0.055 | 0.7% | 7.349 (n=3, 2件除外) |
| test | cold | fusion | 5 | **1.883** | 0.180 | 9.6% | 外れ値なし |
| test | warm | core | 5 | **7.308** | 0.037 | 0.5% | 7.323 (n=4, 1件除外) |
| test | warm | fusion | 5 | **1.695** | 0.043 | 2.5% | 1.676 (n=4, 1件除外) |
| docs generate | warm | core | 3 | **10.172** | 0.999 | 9.8% | n<4のため判定不可 |
| docs generate | warm | fusion ※ | 3 | **0.599** | 0.056 | 9.4% | n<4のため判定不可 |

**※ Fusion の `dbt docs generate` について**：Fusion 2.0.0-preview.183 では `dbt docs generate` 実行時に警告 `[DocsGenerateWarning (dbt1705)]: dbt docs generate is not supported. Use dbt compile --write-catalog to write catalog.json.` を出力し、3回とも 506〜615ms で終了した（exit_code=0）。この値は通常のドキュメント生成処理の所要時間ではなく、Core の値（catalog.json を実際に生成して 10.172秒）と直接比較するのは適切でない。詳細はセクション4の発見Xを参照。

**注意**：外れ値判定は IQR法（Q1-1.5×IQR ～ Q3+1.5×IQR を超える値）を採用。
**2σ法（平均±2σ）では18組すべてで外れ値ゼロ**。これは小標本（n=5）でIQR法が過敏になる特性を反映しており、本サンプルでは「明確な外れ値はない」と解釈するのが妥当。

---

## 2. Fusion の Core に対する速度比

| Command | State | Core avg(秒) | Fusion avg(秒) | 倍率 (Core/Fusion) |
|---|---|--:|--:|--:|
| parse | cold | 5.139 | 1.284 | **4.00×** |
| parse | warm | 4.202 | 0.917 | **4.58×** |
| compile | cold | 7.381 | 1.035 | **7.13×** |
| compile | warm | 5.779 | 0.931 | **6.21×** |
| run | cold | 12.784 | 5.079 | **2.52×** |
| run | warm | 9.770 | 3.576 | **2.73×** |
| test | cold | 7.344 | 1.883 | **3.90×** |
| test | warm | 7.308 | 1.695 | **4.31×** |

**注**：`docs generate` は速度比較から除外。Fusion 側で `dbt docs generate` がサポートされておらず、警告を出して終了するため、Core の所要時間（実際にカタログを生成）との直接比較は意味を持たない。詳細はセクション4の発見Xを参照。

---

## 3. cold vs warm の差分

| Command | Engine | Cold(秒) | Warm(秒) | 差 | 差(%) |
|---|---|--:|--:|--:|--:|
| parse | core | 5.139 | 4.202 | 0.937 | 18.2% |
| parse | fusion | 1.284 | 0.917 | 0.367 | 28.6% |
| compile | core | 7.381 | 5.779 | 1.602 | 21.7% |
| compile | fusion | 1.035 | 0.931 | 0.104 | 10.1% |
| run | core | 12.784 | 9.770 | 3.014 | 23.6% |
| run | fusion | 5.079 | 3.576 | 1.503 | 29.6% |
| test | core | 7.344 | 7.308 | 0.037 | **0.5%** |
| test | fusion | 1.883 | 1.695 | 0.189 | 10.0% |

---

## 4. 主要な発見

### 発見1：Fusion は全コマンドで Core より速い、最大は compile cold で7倍超
- 最大の差は **compile cold（7.13×）**。Fusion の Rust製パーサ・ローカル型チェックの強みが明確に出た
- compile warm も 6.21× と高い倍率を維持
- run の差が 2.5〜2.7× と最も小さい（Snowflake側のクエリ実行時間が支配的になるため、これは予想通り）
- `docs generate` についてはセクション4の発見Xを参照（速度比較対象外）

### 発見2：test の cold/warm 差がほぼゼロ（Core 0.5%、Fusion 10.0%）
- 他コマンドでは cold/warm で18〜30%の差があるが、test はほぼ差がない
- 理由（仮説）：test の処理時間は「Snowflake にテストSQLを投げる時間」が支配的で、dbtプロセスの起動オーバーヘッドが相対的に小さい
- 本に書くべき：**test における cold/warm 差は dbt のキャッシュよりも Snowflake のクエリ実行時間が支配的**

### 発見3：Core の再現性は極めて高い（平均CV 4.4%）、Fusion はバラつきあり（平均CV 16.2%）
- 特に Fusion parse cold は CV 39.1%（0.79〜2.027秒、約2.5倍のレンジ）
- 理由（仮説）：絶対値が小さい（1秒前後）ため、同じ絶対変動でもCV%が大きく見える
- Fusion を本で評価する際は「平均が速い」だけでなく「ばらつきもある」も書くべき

### 発見4：見積もりは1h40m、実測は56分（44分短縮）
- cold phase の70秒待機が予定通り効いた一方、warm phase が想定より高速だった
- 5モデル前後の小規模プロジェクトでは、本計測の所要時間見積もりは保守的に取りすぎたと言える
- 本に書くべき：**自分のプロジェクトで再現する読者向けの参考時間として「86回 × Y1 = 約1時間」と提示可能**

### 発見X：Fusion preview 版の機能制約（dbt docs generate 未サポート）
- Fusion 2.0.0-preview.183 では `dbt docs generate` 実行時に以下の警告が出力される：
  ```
  [warning] [DocsGenerateWarning (dbt1705)]: `dbt docs generate` is not supported.
  Use `dbt compile --write-catalog` to write catalog.json. To host docs locally,
  use the dbt Core index.html with catalog.json and manifest.json in the same directory:
  https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/task/docs/index.html
  ```
- 3回とも 506ms / 515ms / 615ms で終了（exit_code=0）
- Fusion 自身が代替手段として `dbt compile --write-catalog` を案内している
- **実機確認結果（2026-06-26）**：`target_fusion\` 配下に catalog.json も index.html も**存在しないことを確認**
  - catalog.json：Core 7,869B / Fusion 不在
  - index.html：Core 1,818,461B / Fusion 不在
  - manifest.json：両方に存在（Core 627,417B / Fusion 532,011B）。ただしこれは parse/compile/run/test 等の最終実行時に生成されたもので、`dbt docs generate` の成果物ではない
- **確定した事実**：Fusion 2.0.0-preview.183 では `dbt docs generate` が実質的に機能しておらず、catalog.json / index.html は生成されない。catalog.json が必要な場合は Fusion 自身の案内通り `dbt compile --write-catalog` を使う必要がある
- 本に書くべき：**Fusion は preview 段階のため、Core で動くコマンドの一部が未サポート。`dbt docs generate` はその一例で、警告終了で catalog.json / index.html は生成されない。代替として `dbt compile --write-catalog` が案内されている**

---

## 5. 本に書くべき注意点（決定ログとの紐付け）

- **決定2（結果キャッシュ常時有効）**：採用通り。CV%の高低は「結果キャッシュの効果ではない」可能性が高い（4モデル中CREATE系が中心のため）。本に明記する
- **決定8（X3 外れ値併記）**：上記表で実施済み。IQR法は小標本で過敏なので、2σ法（外れ値ゼロ）の方が本サンプルでは妥当である旨も併記推奨
- **決定10（Y1 各回交互、target/分離）**：Core と Fusion で `target_core/` と `target_fusion/` を分離。両エンジンとも `--target-path` をサポートすることをプリフライトで確認済み
- **決定13（test cold は run記録外 + test記録対象）**：手順通り実施。結果として test の cold/warm 差がほぼゼロという興味深い知見が得られた
- **決定14（docs warm 3回のみ）**：n=3は外れ値判定に不十分（IQR法は n≥4）。本では「サンプル数が少ない参考値」と注意書きを入れる。加えて Fusion 側は機能未サポートのため、本計測値は速度比較の対象外（発見X参照）

---

## 6. 次のステップ提案

1. **CSV側で再集計して値の照合**（私が手動抽出した値が CSV と一致するか確認）→ 完了
2. **生成物比較**（実機確認 2026-06-26 完了）
    - **catalog.json**：Core 7,869B / Fusion **存在せず**（発見X 確定）
    - **index.html**：Core 1,818,461B / Fusion **存在せず**（発見X 確定）
    - **manifest.json**：両方に存在。Core 627,417B / Fusion 532,011B（差約95KB、約15%減）
    - **未実施**：manifest.json の構造差分（Fusion で欠けているフィールドまたは形式の違い）。中身比較は別タスク
3. **デイリーログ作成**（本計測日の成果記録）→ 完了
4. **本のドラフト構成検討**（決定11の実務簡略版を含む）
