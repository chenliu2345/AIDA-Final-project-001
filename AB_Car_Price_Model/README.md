# 阿尔伯塔二手车估价模型 / Alberta Used Car Price Model

本项目包含原始 CSV 清洗、SQL Server 入库、CatBoost 训练、模型评估，以及中英文 Windows 桌面程序。

This project provides raw CSV cleaning, SQL Server import, CatBoost training, model evaluation, and Chinese and English Windows desktop applications.

## 1. 直接使用 EXE / Use the EXE directly

打开 `dist` 中的中文版或英文版 EXE 即可使用。两个 EXE 允许提交，不需要为了估价重新训练。

Open either the Chinese or English EXE in `dist`. Both executables are eligible for Git submission; valuation does not require retraining.

| 文件 / File | 语言 / Language |
| --- | --- |
| `AB_Car_Price.exe` | 中文 / Chinese |
| `AB_Car_Price_EN.exe` | 英文 / English |

两个 EXE 使用相同模型和预测逻辑。只需发送所需语言的一个 EXE，即可提供估价功能；模型、误差报告、城市距离和 Python 依赖已内置。需要 Windows 64 位系统，无需安装 Python 或 SQL Server，也无需原始 CSV 或网络。

Both EXEs use the same model and prediction logic. Distribute only the EXE for the desired language to provide valuation: the model, error report, city distances and Python dependencies are bundled. A 64-bit Windows system is required; prediction needs no Python installation, SQL Server, raw CSV or network connection.

- 第一页展示总体及各实际价位的 N、MAE、MedAE、R² 和 Relative MAE。  
  The opening page shows N, MAE, MedAE, R² and Relative MAE overall and by actual-price band.
- 第二页填写车型、年份、里程等，具体预测价格与历史误差参考范围并排显示。  
  Enter model, year, mileage and other details on the second page; the point prediction and historical error range appear side by side.
- 第三页支持从原始 CSV 重新训练，需要下文所列的额外依赖。  
  The third page supports retraining from raw CSV, with the additional requirements listed below.

程序优先读取 EXE 同级 `models/current.json` 指向的外置模型；没有外置模型时使用内置模型。也可以从界面选择兼容的模型目录。

The application first looks for an external model referenced by `models/current.json` beside the EXE, then falls back to its bundled model. A compatible model folder can also be selected in the interface.

## 2. 当前模型及评估口径 / Current model and evaluation

| 项目 / Item | 当前配置 / Current configuration |
| --- | --- |
| 原始数据 / Raw data | `data/Alberta_owner_sales_car.csv`，63,377 条 / 63,377 rows |
| ETL 接受记录 / ETL-accepted records | 62,643 条 / 62,643 rows |
| 训练筛选 / Training filter | 43,054 条 SOLD / 43,054 SOLD rows |
| 划分 / Split | 随机 80/20，种子 42 / Random 80/20, seed 42 |
| 模型 / Model | CatBoost，固定 2,000 轮，不提前停止 / CatBoost, fixed 2,000 iterations, no early stopping |
| 评估训练集 / Evaluation training set | 34,443 条 / 34,443 rows |
| 留出测试集 / Held-out test set | 8,611 条 / 8,611 rows |
| 测试 R² / Test R² | 0.511741 |
| 测试 MAE / Test MAE | $4,345.19 CAD |
| 测试 RMSE / Test RMSE | $14,141.97 CAD |

部署模型在全部 43,054 条 SOLD 上拟合。当前发布版本未应用后续价格异常 V2 的自动排除。

The deployment model is fitted on all 43,054 SOLD rows. The currently published version does not apply the later price-anomaly V2 automatic exclusions.

表中是完整留出测试集结果，不是共同测试子集分数，也不是全量模型对已见样本的回评分数。随机行划分可能让同车记录跨集合，因此不等同于全新车辆泛化评估。SOLD 记录的标签仍是采集的挂牌价格，非核实成交价。

These are results for the complete held-out test set, not the common test subset or full-data model rescoring on seen rows. Random row splits may share records of the same car across partitions, so these metrics do not measure generalization to entirely unseen cars. SOLD labels remain scraped listing prices, not verified transaction prices.

范围根据“实际价格减预测价格”的历史残差计算：按预测价位取 10%–90% 分位数，稀疏档回退整体残差，下限不低于零，向外取整至百元。留出集另分为 4,305 条校准记录与 4,306 条覆盖率验证记录，验证覆盖率约 78.7%。

Ranges use historical residuals, defined as actual minus predicted price. The 10th–90th percentiles are selected by predicted-price band, with pooled residuals for sparse bands, a zero lower floor and outward rounding to hundreds of dollars. The held-out set is further divided into 4,305 calibration rows and 4,306 coverage-validation rows, with approximately 78.7% validation coverage.

覆盖率来自评估模型；全量部署模型的区间仅作参考，不是成交保证，也不是固定 ±百分比。

Coverage is measured with the evaluation model. Intervals for the full-data deployment model are references only, not sale-price guarantees or fixed percentage adjustments.

Relative MAE = MAE / 平均实际价格，非 MAPE。窄价档的 R² 可能为负，应结合 MAE、MedAE 和样本数理解。

Relative MAE = MAE / mean actual price; it is not MAPE. R² can be negative within narrow price bands and should be interpreted alongside MAE, MedAE and sample count.

## 3. 重新训练的依赖 / Retraining requirements

通过 EXE 重新训练需要：

Retraining through the EXE requires:

1. 符合原爬虫字段结构的 CSV。  
   A CSV matching the original scraper field structure.
2. 可访问的 SQL Server 实例；默认实例为 `.`，使用 Windows 身份验证。  
   An accessible SQL Server instance; the default is `.`, using Windows authentication.
3. Microsoft ODBC Driver 17 或 18 for SQL Server。  
   Microsoft ODBC Driver 17 or 18 for SQL Server.
4. 本项目所需的建库、建表和写入权限，以及可写的模型输出目录。  
   Database creation, table creation and write permissions required by this project, plus a writable model output folder.
5. 出现未缓存城市时，需要网络和 `ORS_API_KEY`。  
   A network connection and `ORS_API_KEY` when uncached cities require road distances.

数据库名称为 `AB_Car_Price_Model`。程序运行 `schema.sql` 并保存清洗和导入审计。数据库由 SQL Server 管理，不保存在本项目目录；移动文件夹不会迁移数据库。

The database is named `AB_Car_Price_Model`. The application runs `schema.sql` and saves cleaning and import audits. SQL Server manages the database outside this project folder; moving the folder does not move the database.

密钥放在 EXE 同级 `.env` 中；当 EXE 位于 `dist` 时，也读取项目根目录 `.env`。下面仅为占位示例，不是真实密钥。

Place the key in `.env` beside the EXE. When the EXE is inside `dist`, the project-root `.env` is also read. The following is a placeholder, not a real key.

```powershell
Copy-Item .env.example .env
```

只在自己电脑上的 `.env` 填入真实密钥；源码、README、日志、模型和 EXE 都不应包含密钥。`.gitignore` 排除这些密钥配置，但不会删除本地 `.env`。

Enter your real key only in the local `.env`. Source, README, logs, models and EXEs must not contain it. `.gitignore` excludes secret configuration without deleting your local `.env`.

```dotenv
ORS_API_KEY=your_own_key
```

预测不需要密钥。不要分发实际 `.env`，也不要提交任何真实密钥或原始数据。

Prediction does not require a key. Do not distribute the real `.env` or commit any actual keys or raw data.

### 原始 CSV 格式示例 / Raw CSV format example

可下载或打开 [data/example.csv](data/example.csv)。它的 15 个字段名称和顺序与真实原始 CSV 完全一致，包含 3 条手工虚构记录，不含任何真实挂牌数据或密钥，允许提交。

Open or download [data/example.csv](data/example.csv). Its 15 column names and their order exactly match the original raw CSV. It contains three manually invented records, no real listings or secrets, and is eligible for submission.

示例展示了 SOLD 和 ACTIVE 状态、未售出时留空的 Sold_Date，以及含逗号的字段如何使用 CSV 引号。价格使用 CAD 数字，不带货币符号；日期采用 YYYY-MM-DD；Model 使用“年份 车型, 配置”的格式，以便 ETL 提取年份、车型和配置。

The example demonstrates SOLD and ACTIVE statuses, a blank Sold_Date for an unsold car, and CSV quoting for fields containing commas. Prices are numeric CAD amounts without currency symbols; dates use YYYY-MM-DD; Model follows "year model, trim" so ETL can extract the year, model and trim.

这只是格式示例，不用于训练或评估。训练至少需要 100 条合格 SOLD 记录。请把自己的真实数据另存为 `data/Alberta_owner_sales_car.csv`，不要用真实数据覆盖可提交的 `example.csv`。

This is a format example, not training or evaluation data. Training requires at least 100 eligible SOLD rows. Save your actual data separately as `data/Alberta_owner_sales_car.csv`; do not overwrite the committable `example.csv` with real data.

## 4. 源码运行、训练与打包 / Run, train and build from source

下载后有两条使用路径：直接运行 `dist` 中的 EXE，或者在自己的电脑上用 `requirements.txt` 创建新环境后运行源码。两条路径使用同一份已训练模型，无需先取得原始数据或重新训练。

After downloading, either run an EXE in `dist` or create your own environment from `requirements.txt` and run the source. Both paths use the same trained model; neither requires raw data or retraining for prediction.

`.venv/`、`.venv-user/` 和 `__pycache__/` 不随 Git 提交。环境由下面的命令创建，缓存由 Python 自动生成；两个 EXE 不依赖这些外置目录。

`.venv/`, `.venv-user/` and `__pycache__/` are not committed to Git. The commands below create the environment, and Python regenerates its caches; neither EXE depends on these external folders.

源码方式：先安装 Windows 64 位 Python 3.11（含 Python Launcher），进入项目目录，再执行以下命令。`.venv-user` 是用户自己创建的环境，不依赖作者的 `.venv`。以下路径为示例，请替换为实际位置。

For source execution, install 64-bit Python 3.11 for Windows with the Python Launcher, open the project directory, then run the commands below. `.venv-user` is your own newly created environment and does not depend on the author's `.venv`. Replace the example path with your actual location.

```powershell
Set-Location 'D:\your-clone\AB_Car_Price_Model'
py -3.11 -m venv .venv-user
.\.venv-user\Scripts\python.exe -m pip install -r requirements.txt
.\.venv-user\Scripts\python.exe app.py
```

源码直接预测必须保留 `models/current.json` 和它指向版本目录中的 `price_model.cbm`、`metadata.json`；这些文件允许提交。EXE 已内置这些资源，可单独使用。仅预测不需要 SQL Server、原始 CSV、ORS 密钥或网络；首次安装 Python 依赖需要获取相应安装包。

Source prediction requires `models/current.json` and the referenced version's `price_model.cbm` and `metadata.json`; these files are eligible for submission. Each EXE bundles these resources and works independently. Prediction needs no SQL Server, raw CSV, ORS key or network access; initial dependency installation requires obtaining the packages.

此设置不更换模型、不重新训练、不改动 CatBoost 参数、特征或区间计算，保持原预测结果及模型性能。启动速度会受电脑硬件和 EXE 解包影响。

This setup does not replace or retrain the model or change CatBoost parameters, features or interval calculations, preserving predictions and model performance. Startup time depends on the computer and EXE extraction.

要训练模型，还需按照第 3 节准备 SQL Server、ODBC 驱动及数据库权限。自行获取有权使用且字段匹配的原始 CSV，放入本地 `data/Alberta_owner_sales_car.csv`；数据不会随 Git 下载。遇到未缓存城市时，使用自己的 ORS 密钥创建本地 `.env`。不要提交该文件或原始数据。

To train a model, also prepare SQL Server, the ODBC driver and database permissions from section 3. Obtain an authorized raw CSV with the required fields and place it at local `data/Alberta_owner_sales_car.csv`; Git does not supply the data. Create a local `.env` with your own ORS key for uncached cities. Never commit this file or the raw data.

环境可用且模型文件完整时，可直接启动中文源码界面进行预测。仅在没有可用模型时才需要先训练。

With a working environment and complete model files, start the Chinese source interface directly for predictions. Training is needed only when no usable model is available.

```powershell
.\.venv-user\Scripts\python.exe app.py
```

从原始 CSV 执行 ETL、SQL 入库、评估及全量训练：

Run ETL, SQL import, evaluation and full-data training from raw CSV:

```powershell
.\.venv-user\Scripts\python.exe train.py --csv data\Alberta_owner_sales_car.csv --output models --server .
```

训练固定为 2,000 轮。新版本保存并校验后才更新 `models/current.json`。输出包含模型、元数据、分档误差、测试预测、区间审计、划分清单和清洗审计。

Training uses exactly 2,000 iterations. A new version is saved and validated before updating `models/current.json`. Outputs include models, metadata, price-band errors, test predictions, interval audits, split manifests and cleaning audits.

| 文件 / File | 内容 / Contents |
| --- | --- |
| `price_band_metrics.csv` | 分价位与总体指标 / Price-band and overall metrics |
| `test_predictions.csv` | 留出测试预测 / Held-out predictions |
| `interval_audit.csv` | 区间校准与验证记录 / Interval calibration and validation records |

当 `models/current.json` 指向完整可用模型时，可以直接跳过训练并打包两种语言；模型允许提交，无需每次重新训练。

When `models/current.json` references a complete model, build both languages without retraining. Models are eligible for submission, so retraining is not required for every build.

```powershell
.\.venv-user\Scripts\python.exe build.py --skip-train --language zh
.\.venv-user\Scripts\python.exe build.py --skip-train --language en
```

也可以在准备好依赖、数据和数据库后，一次训练并打包中文版本，再复用生成的模型打包英文版本：

Alternatively, after preparing dependencies, data and the database, train and build the Chinese version in one step, then reuse the resulting model to build the English version:

```powershell
.\.venv-user\Scripts\python.exe build.py --csv data\Alberta_owner_sales_car.csv --server . --language zh
.\.venv-user\Scripts\python.exe build.py --skip-train --language en
```

`train_and_build.cmd` 支持相同参数。英文源码由 `build_english.py` 和 `translations_en.json` 从共用源码生成，位于 `.packaging/english_source`，无需单独维护预测逻辑。源码说明采用中文在前、英文紧随其后的格式；程序界面语言独立于注释语言。

`train_and_build.cmd` accepts the same arguments. `build_english.py` and `translations_en.json` generate English source from shared code into `.packaging/english_source`, avoiding separate prediction logic. Source documentation places Chinese first and English immediately after; interface language is independent of comment language.

## 5. 文件结构 / Project structure

下表列出项目结构。是否排除按第 7 节的具体文件判断，不按目录或扩展名一概判断。

This table describes the project structure. Exclusions are decided by the specific files in section 7, not by blanket directory or extension rules.

| 路径 / Path | 用途 / Purpose |
| --- | --- |
| `README.md` | 中英文使用说明 / Chinese and English usage guide |
| `.gitignore` | 本项目独立忽略规则 / Project-specific ignore rules |
| `dist/` | 中英文 EXE / Chinese and English EXEs |
| `data/` | 原始 CSV 按具体文件排除 / Raw CSV excluded by exact file path |
| `models/` | 模型与汇总结果可提交，逐条车辆数据另行排除 / Models and summaries allowed; per-listing data excluded individually |
| `models/current.json` | 当前模型版本指针 / Current model version pointer |
| `models/random2000_63377_ranges_v1/` | 已选固定轮数模型 / Selected fixed-iteration model |
| `models/comparisons/` | 多方案比较结果 / Multi-scheme comparison results |
| `models/price_anomaly_v2/` | 异常审计与清洗比较 / Anomaly audit and cleaning comparisons |
| `app.py` | 桌面界面 / Desktop interface |
| `predict.py` | 预测与输入检查 / Prediction and input validation |
| `model_quality.py` | 分档误差与区间 / Price-band errors and intervals |
| `etl.py` | 最终版对齐清洗 / Final-submission-aligned cleaning |
| `schema.sql`, `database.py` | SQL 结构与入库 / SQL schema and import |
| `distances.py` | 道路距离与缓存 / Road distances and cache |
| `train.py` | CSV 到模型训练入口 / CSV-to-model training entry point |
| `build.py`, `launcher.py` | EXE 构建与启动 / EXE build and launch |
| `build_english.py`, `translations_en.json` | 英文源码生成与翻译 / English source generation and translations |
| `compare_training.py` | 多方案训练比较 / Training-scheme comparison |
| `price_anomaly.py` | 异常审计和清洗对比 / Anomaly audit and cleaning comparison |
| `publish_selected_model.py` | 发布已选模型与报告 / Publish selected model and reports |
| `test_*.py`, `verify_*.py` | 测试与独立校验 / Tests and independent verification |
| `requirements.txt` | Python 依赖 / Python dependencies |
| `.env` | 本地密钥，不提交或分发 / Local keys, never committed or distributed |
| `.venv/`, `.venv-user/` | 本地虚拟环境，不提交；按 requirements.txt 重建 / Local environments, excluded; recreate from requirements.txt |
| `__pycache__/` | 自动生成的 Python 缓存，不提交 / Automatically generated Python cache, excluded |
| `.packaging/` | 打包临时文件与自检，不提交；构建时自动生成 / Packaging files and self-tests, excluded; generated during builds |

## 6. 验证 / Verification

已实际建立全新 Python 3.11 虚拟环境，仅通过 `requirements.txt` 安装依赖，且 `pip check` 通过。在仅含源码和当前模型、不含原始数据、`.env` 或作者虚拟环境的临时副本中，自检通过，具体预测价格与区间和现有 EXE 完全一致。此次配置更新未修改模型和预测逻辑。

A fresh Python 3.11 virtual environment was created using only `requirements.txt`, and `pip check` passed. A temporary copy containing only source and the current model, without raw data, `.env` or the author's environment, passed its self-test. Its point prediction and interval exactly matched the existing EXE. This setup update did not change the model or prediction logic.

依赖安装后可运行计算测试；下面两个 EXE 自检命令仅在本机完成对应打包或取得已打包 EXE 后可用。

Run calculation tests after installing dependencies. The two EXE self-test commands below require the corresponding applications to have been built locally or obtained separately.

```powershell
.\.venv-user\Scripts\python.exe -m unittest test_model_quality
.\dist\AB_Car_Price.exe --self-test .packaging\check_zh.json
.\dist\AB_Car_Price_EN.exe --self-test .packaging\check_en.json
```

构建入口会自动自检相应 EXE。两版已核对模型摘要与默认车辆预测一致；本次整目录迁移后也重新验证通过。这不代表已在所有全新 Windows 环境测试。

The build entry point automatically tests its EXE. Both languages have matching model hashes and default-vehicle predictions, and both passed validation after the whole-folder migration. This does not imply testing on every clean Windows environment.

第 2 节指标描述当前已验证模型。模型和汇总结果允许提交；用于重新训练的原始/逐条车辆数据不提交。重新训练需要另行准备数据和 SQL Server，使用不同数据时会重新计算指标。

Section 2 describes the currently validated model. Models and aggregate results are eligible for submission; raw/per-listing training data is excluded. Retraining requires separately supplied data and SQL Server, and different data produces newly computed metrics.

## 7. Git 提交范围 / Git submission scope

本目录 `.gitignore` 先用 `!*/` 和 `!*` 取消父目录忽略规则的影响，再按确切路径排除包含原始车辆数据的文件。按用户要求另外排除本地虚拟环境 `.venv/`、`.venv-user/` 和各层级 `__pycache__/`。打包目录 `.packaging/` 也按用户要求排除，它可在构建时重新生成，运行源码和 EXE 均不依赖它。没有 `*.csv`、`/models/` 或 `/dist/` 整类排除规则。

This folder's `.gitignore` first overrides parent ignore rules with `!*/` and `!*`, then excludes exact paths containing source vehicle data. As requested, local environments `.venv/` and `.venv-user/`, plus `__pycache__/` at any depth, are also excluded. The `.packaging/` directory is also excluded as requested; builds regenerate it, and neither source prediction nor EXE execution depends on it. There are no blanket exclusions for `*.csv`, `/models/` or `/dist/`.

判断依据是文件字段与内容，而不是名称：原始 CSV、保留车辆记录的 ETL 输出、测试数据、带真实价格的逐条预测和异常审计不提交；仅含 R²、MAE、样本数等汇总统计的 comparison 和分价位报告可提交。含 Listing_Key、Group_Key 或 Source_Row 的逐条划分清单也必须排除；它们仍可关联到原始车辆记录。两个已确认保存逐条数据的 PKL 快照按具体路径排除，其他模型文件不排除。所有实际密钥配置和私钥继续排除；`.env.example` 仅含空值，可以提交。

Decisions use fields and content rather than filenames. Raw CSV, ETL vehicle records, test datasets, per-listing predictions containing actual prices and anomaly audits are excluded. Comparisons and price-band reports containing only aggregate statistics such as R², MAE and sample counts are allowed. Per-row split manifests containing Listing_Key, Group_Key or Source_Row must also be excluded because they remain associated with source vehicle records. The two verified per-listing PKL snapshots are excluded by exact path; other model files are allowed. Actual secret configuration and private-key files remain excluded; `.env.example` contains only an empty value and is eligible for submission.

新生成的文件需要按相同标准单独检查并更新具体路径；Git 不会自动判断文件内容。忽略规则不删除本地文件，本次也未执行提交或上传。

Newly generated files require individual review and corresponding exact-path updates; Git cannot classify their content automatically. Ignore rules do not delete local files, and this update does not commit or upload anything.

### CSV 逐文件判断 / Per-file CSV decisions

| 文件 / File | 规则 / Decision | 依据 / Evidence |
| --- | --- | --- |
| `data/example.csv` | 可提交 / Allow | 3 条手工虚构数据，表头与原始文件一致 / Three invented rows with the original header |
| `data/Alberta_owner_sales_car.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing title`, `Price(CA$)` |
| `models/51edd0dd-e036-481d-98c3-96e07605fbd9/etl_cleaned.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Listing_Title`, `Price_CAD`, `Scrape_Date` |
| `models/51edd0dd-e036-481d-98c3-96e07605fbd9/rejected_rows.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Raw_JSON` |
| `models/51edd0dd-e036-481d-98c3-96e07605fbd9/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/51edd0dd-e036-481d-98c3-96e07605fbd9/test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/6610f277-7bed-4f2a-b30a-a48f232a7daf/rejected_rows.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Raw_JSON` |
| `models/6610f277-7bed-4f2a-b30a-a48f232a7daf/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/6610f277-7bed-4f2a-b30a-a48f232a7daf/test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/a3a3d256-83d9-494d-a094-be4589e7aa8a/deployment_training_subset_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/a3a3d256-83d9-494d-a094-be4589e7aa8a/etl_cleaned.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Listing_Title`, `Price_CAD`, `Scrape_Date` |
| `models/a3a3d256-83d9-494d-a094-be4589e7aa8a/rejected_rows.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Raw_JSON` |
| `models/a3a3d256-83d9-494d-a094-be4589e7aa8a/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/a3a3d256-83d9-494d-a094-be4589e7aa8a/test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/comparisons/self_project_63377/comparison.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/comparisons/self_project_63377/etl_cleaned.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Listing_Title`, `Price_CAD`, `Scrape_Date` |
| `models/comparisons/self_project_63377/grouped_70_15_15_es/common_test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/comparisons/self_project_63377/grouped_70_15_15_es/price_band_metrics.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/comparisons/self_project_63377/grouped_70_15_15_es/price_band_metrics_common_test.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/comparisons/self_project_63377/grouped_70_15_15_es/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/comparisons/self_project_63377/grouped_70_15_15_es/test_dataset.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Price_CAD` |
| `models/comparisons/self_project_63377/grouped_70_15_15_es/test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/comparisons/self_project_63377/my_scheme/common_test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/comparisons/self_project_63377/my_scheme/price_band_metrics.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/comparisons/self_project_63377/my_scheme/price_band_metrics_common_test.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/comparisons/self_project_63377/my_scheme/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/comparisons/self_project_63377/my_scheme/test_dataset.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Price_CAD` |
| `models/comparisons/self_project_63377/my_scheme/test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/comparisons/self_project_63377/original_scheme/common_test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/comparisons/self_project_63377/original_scheme/price_band_metrics.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/comparisons/self_project_63377/original_scheme/price_band_metrics_common_test.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/comparisons/self_project_63377/original_scheme/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/comparisons/self_project_63377/original_scheme/test_dataset.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Price_CAD` |
| `models/comparisons/self_project_63377/original_scheme/test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/comparisons/self_project_63377/random_80_20_es/common_test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/comparisons/self_project_63377/random_80_20_es/price_band_metrics.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/comparisons/self_project_63377/random_80_20_es/price_band_metrics_common_test.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/comparisons/self_project_63377/random_80_20_es/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/comparisons/self_project_63377/random_80_20_es/test_dataset.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Price_CAD` |
| `models/comparisons/self_project_63377/random_80_20_es/test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/comparisons/self_project_63377/rejected_rows.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Raw_JSON` |
| `models/price_anomaly_v2/anomaly_summary.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/comparison.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/database_clean_snapshot.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing_Title`, `Price_CAD` |
| `models/price_anomaly_v2/grouped_70_15_15_es/before_after_test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/price_anomaly_v2/grouped_70_15_15_es/database_clean_v2.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing_Title`, `Price_CAD` |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_anomaly_decisions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_auto_excluded.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_auto_excluded_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_band_comparison.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_band_original_test_before.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_band_original_test_v2.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_band_retained_test_before.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_band_retained_test_v2.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_manual_review.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_manual_review_extreme_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/grouped_70_15_15_es/price_manual_review_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/grouped_70_15_15_es/sold_training_v2.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing_Title`, `Price_CAD` |
| `models/price_anomaly_v2/grouped_70_15_15_es/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/price_anomaly_v2/known_cases.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/my_scheme/before_after_test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/price_anomaly_v2/my_scheme/database_clean_v2.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing_Title`, `Price_CAD` |
| `models/price_anomaly_v2/my_scheme/price_anomaly_decisions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/my_scheme/price_auto_excluded.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/my_scheme/price_auto_excluded_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/my_scheme/price_band_comparison.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/my_scheme/price_band_original_test_before.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/my_scheme/price_band_original_test_v2.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/my_scheme/price_band_retained_test_before.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/my_scheme/price_band_retained_test_v2.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/my_scheme/price_manual_review.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/my_scheme/price_manual_review_extreme_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/my_scheme/price_manual_review_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/my_scheme/sold_training_v2.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing_Title`, `Price_CAD` |
| `models/price_anomaly_v2/my_scheme/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/price_anomaly_v2/original_scheme/before_after_test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/price_anomaly_v2/original_scheme/database_clean_v2.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing_Title`, `Price_CAD` |
| `models/price_anomaly_v2/original_scheme/price_anomaly_decisions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/original_scheme/price_auto_excluded.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/original_scheme/price_auto_excluded_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/original_scheme/price_band_comparison.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/original_scheme/price_band_original_test_before.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/original_scheme/price_band_original_test_v2.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/original_scheme/price_band_retained_test_before.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/original_scheme/price_band_retained_test_v2.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/original_scheme/price_manual_review.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/original_scheme/price_manual_review_extreme_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/original_scheme/price_manual_review_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/original_scheme/sold_training_v2.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing_Title`, `Price_CAD` |
| `models/price_anomaly_v2/original_scheme/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/price_anomaly_v2/price_band_comparison_all_schemes.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/random_80_20_es/before_after_test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |
| `models/price_anomaly_v2/random_80_20_es/database_clean_v2.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing_Title`, `Price_CAD` |
| `models/price_anomaly_v2/random_80_20_es/price_anomaly_decisions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/random_80_20_es/price_auto_excluded.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/random_80_20_es/price_auto_excluded_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/random_80_20_es/price_band_comparison.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/random_80_20_es/price_band_original_test_before.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/random_80_20_es/price_band_original_test_v2.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/random_80_20_es/price_band_retained_test_before.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/random_80_20_es/price_band_retained_test_v2.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/price_anomaly_v2/random_80_20_es/price_manual_review.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/random_80_20_es/price_manual_review_extreme_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/random_80_20_es/price_manual_review_top30.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price`, `Kilometres`, `Link`, `Listing_Title` |
| `models/price_anomaly_v2/random_80_20_es/sold_training_v2.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Kilometres`, `Link`, `Listing_Title`, `Price_CAD` |
| `models/price_anomaly_v2/random_80_20_es/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/random2000_63377_ranges_v1/interval_audit.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Actual_Price_CAD` |
| `models/random2000_63377_ranges_v1/price_band_metrics.csv` | 可提交 / Allow | 仅汇总统计指标 / Aggregate statistics only |
| `models/random2000_63377_ranges_v1/split_manifest.csv` | 不提交 / Exclude | 含 Listing_Key 等逐条标识 / Contains per-listing identifiers such as Listing_Key |
| `models/random2000_63377_ranges_v1/test_predictions.csv` | 不提交 / Exclude | 包含逐条原始车辆字段 / Contains per-listing source fields: `Price_CAD` |

### 其他明确排除文件 / Other explicitly excluded files

- `models/comparisons/self_project_63377/etl_checkpoint.pkl`：逐条训练数据快照。 / Per-listing training-data snapshot.
- `models/comparisons/self_project_63377/training_frame.pkl`：逐条训练数据快照。 / Per-listing training-data snapshot.
- `.env`：真实密钥。 / Actual secret.

提交前可核验以下路径；请勿强制添加被排除的真实数据或密钥。

Verify these paths before committing; do not force-add excluded source data or secrets.

```powershell
git check-ignore -v -- .env data/Alberta_owner_sales_car.csv
git status --short -- dist models/comparisons/self_project_63377/comparison.csv
```

## AI 使用声明 / AI assistance declaration

本项目使用 AI 辅助代码开发。AI 不提供原始车辆数据，也不为项目结论背书。完整说明见 [AI 使用声明](AI_DECLARATION.md)。

This project uses AI-assisted code development. AI does not provide the original vehicle data or endorse project conclusions. See the full [AI assistance declaration](AI_DECLARATION.md).
