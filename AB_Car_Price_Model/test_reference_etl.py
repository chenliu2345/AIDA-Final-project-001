import ast
from contextlib import redirect_stdout
import hashlib
import io
import logging
import os
from pathlib import Path
import re
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch
from difflib import SequenceMatcher

import pandas as pd
import distances

from etl import clean_csv, process_reposts
from train import split_holdout

REFERENCE = Path(os.environ.get("AB_ETL_REFERENCE", Path(__file__).resolve().parent.parent / "AIDA-Final-project-001" / "Final_Submit_Version" / "ETL_Engine.py"))
RAW_CSV = Path(os.environ.get("AB_ETL_RAW_CSV", Path(__file__).parent / "data" / "Alberta_owner_sales_car.csv"))


def reference_result(path, directory):
    """直接运行最终提交版的原始函数作为对照，不通过修正版的函数计算期望结果。
    Run original final-submission functions directly rather than deriving expectations from revised functions."""
    names = {"clean_km", "clean_price", "clean_str", "extract_year_brand", "get_title_similarity",
             "process_reposts", "clean_car_sales_data", "optimize_car_data", "_normalize_body_style",
             "_clean_kilometres", "_hash_url", "transform", "validate"}
    functions = [node for node in ast.parse(REFERENCE.read_text(encoding="utf-8-sig")).body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
    logger = logging.getLogger("reference_test")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    rejected = []
    namespace = {"pd": pd, "os": os, "re": re, "hashlib": hashlib, "SequenceMatcher": SequenceMatcher,
                 "Engine": object, "log": logger, "MASTER_FILENAME": str(directory / "raw.csv"),
                 "_DOOR_SUFFIX_RE": re.compile(r",\s*(?:\d+|OTHER)\s+DOORS?$", re.IGNORECASE),
                 "PRICE_MIN": 1, "PRICE_MAX": 10_000_000, "KMS_MIN": 0, "KMS_MAX": 2_000_000,
                 "YEAR_MIN": 1900, "YEAR_MAX": 2027, "KMS_BLACKLIST": {1,99,123,1234,12345,123456,999999,111111},
                 "VALID_STATUSES": {"SOLD","ACTIVE","ACTIVE_REPOST","RESHELVED"},
                 "VALID_CONDITIONS": {"USED","DAMAGED","SALVAGE","LEASE TAKEOVER","UNKNOWN"},
                 "VALID_TRANS": {"AUTOMATIC","MANUAL","SEMI-AUTOMATIC","OTHER","UNKNOWN"},
                 "TABLE": {"rejected": "ignored"}, "_write_rejected": lambda df, engine, source: rejected.append(df.copy())}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(REFERENCE), "exec"), namespace)
    shutil.copy2(path, directory / "raw.csv")
    with redirect_stdout(io.StringIO()):
        namespace["process_reposts"]()
        namespace["clean_car_sales_data"](str(directory / "raw.csv"), str(directory / "filled.csv"))
        namespace["optimize_car_data"](str(directory / "filled.csv"), str(directory / "optimized.csv"))
        transformed = namespace["transform"](pd.read_csv(directory / "optimized.csv", encoding="utf-8"))
        transformed["Source_Row"] = range(1, len(transformed) + 1)
        accepted = namespace["validate"](transformed, None, str(path))
    return accepted, rejected[0] if rejected else pd.DataFrame(columns=["Source_Row", "Reject_Reason"])


def raw_row(**changes):
    """创建具备全部原始字段的有效车辆样本，供边界和重发识别测试使用。
    Build valid raw vehicle samples for boundary and repost tests."""
    row = {"Listing title": "2019 Toyota RAV4 XLE", "Model": "2019 Toyota RAV4, XLE", "Price(CA$)": "25000",
           "Link": "https://www.kijiji.ca/v-car/12345678", "Location": "Red Deer", "Scrape_Date": "2026-01-01",
           "Status": "Sold", "Sold_Date": "2026-01-15", "Condition": "Used", "Kilometres": "65,000",
           "Transmission": "Automatic", "Drivetrain": "AWD", "Seats": "5 seats", "Body Style": "SUV, Crossover, 5 doors",
           "Colour": "White exterior"}
    return {**row, **changes}


class PipelineTests(unittest.TestCase):
    """以原始代码逐行对照，覆盖清洗语义、重发边界、随机划分和原版模型预测一致性。
    Compare original and revised code row by row across ETL, reposts, splits and predictions."""

    def assert_reference_parity(self, path):
        """比较所有原版持久化字段及拒绝原因，完整 URL 只比较其哈希，不保存明文链接。
        Compare all persisted fields and rejection reasons; compare full URLs only by hash without saving plaintext links."""
        actual, audit = clean_csv(path)
        with tempfile.TemporaryDirectory() as directory:
            expected, rejected = reference_result(path, Path(directory))
        expected = expected.rename(columns={"Condition": "Condition_Label", "Transmission": "Transmission_Type",
                                           "Drivetrain": "Drivetrain_Type", "Seats": "Seats_Count"}).drop(columns=["Link_URL"])
        pd.testing.assert_frame_equal(actual[expected.columns].reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False)
        self.assertEqual({r["Source_Row"]: r["Reject_Reason"] for r in audit if r["Reject_Reason"]},
                         dict(zip(rejected["Source_Row"], rejected["Reject_Reason"])))
        return actual

    @unittest.skipUnless(REFERENCE.is_file() and RAW_CSV.is_file(), "原版源码或完整原始 CSV 不在此机器上")
    def test_full_original_csv_parity(self):
        """将完整实际爬虫 CSV 分别交给原版和修正版，逐行比较清洗结果。
        Run the full real scraped CSV through both pipelines and compare each cleaned row."""
        result = self.assert_reference_parity(RAW_CSV)
        self.assertIn("OTHER", set(result["Base_Model"]))
        self.assertIn("ACTIVE_REPOST", set(result["Status"]))

    @unittest.skipUnless(REFERENCE.is_file(), "原版源码不在此机器上")
    def test_original_rules_and_boundaries(self):
        """覆盖频数 9/10、1900 年、价格上下限、公里上下限、未知里程及非法枚举等原版边界。
        Cover original frequency, year, price, mileage, unknown-mileage and invalid-enumeration boundaries."""
        rows = [raw_row() for _ in range(20)]
        rows += [raw_row(Model="2018 Rare Car, Rare Trim", Location="Rare City") for _ in range(9)]
        rows += [raw_row(Model="1900 Ford Model T, BASE", **{"Price(CA$)": "1", "Kilometres": "2000000"}) for _ in range(10)]
        rows += [raw_row(**change) for change in [
            {"Kilometres": "pending"}, {"Kilometres": "999999"}, {"Kilometres": "2000001"},
            {"Price(CA$)": "10000000"}, {"Price(CA$)": "10000001"}, {"Price(CA$)": "$25000"},
            {"Model": "Unknown", "Listing title": "2019 Toyota RAV4"}, {"Status": "Invalid"},
            {"Status": "Active_Repost"}, {"Status": "Reshelved"}, {"Condition": "Invalid"}, {"Transmission": "Invalid"},
        ]]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.csv"
            pd.DataFrame(rows).to_csv(path, index=False)
            result = self.assert_reference_parity(path)
        self.assertIn(1900, result.Year.tolist())
        self.assertIn(0, result.Kilometres.tolist())
        self.assertIn(10000000, result.Price_CAD.tolist())
        self.assertGreater(result["Listing_Key"].duplicated().sum(), 0)

    def test_repost_time_window(self):
        """相同车辆在一天内重发应标记；相隔两天不能误判。
        Flag same-car reposts within one day but not two days apart."""
        old = raw_row(**{"Sold_Date": "2026-01-14"})
        new = raw_row(**{"Status": "Active", "Scrape_Date": "2026-01-15", "Sold_Date": "", "Link": "new"})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.csv"
            pd.DataFrame([old, new]).to_csv(path, index=False)
            process_reposts(str(path))
            self.assertEqual(pd.read_csv(path).Status.tolist(), ["Reshelved", "Active_Repost"])
            old["Sold_Date"] = "2026-01-13"
            pd.DataFrame([old, new]).to_csv(path, index=False)
            process_reposts(str(path))
            self.assertEqual(pd.read_csv(path).Status.tolist(), ["Sold", "Active"])

    def test_original_split(self):
        """逐个索引比较原版随机 80/20 划分，保证训练行和测试行互不重叠。
        Compare every random 80/20 split index and confirm disjoint training and test rows."""
        from sklearn.model_selection import train_test_split
        frame = pd.DataFrame({"Group_Key": [str(i // 3) for i in range(300)]})
        parts = split_holdout(frame)
        expected = train_test_split(list(range(300)), test_size=0.2, random_state=42)
        self.assertEqual([part.tolist() for part in parts], expected)
        self.assertEqual(sorted(int(i) for part in parts for i in part), list(range(300)))
        self.assertFalse(set(parts[0]) & set(parts[1]))

    def test_distance_cache_and_missing_key(self):
        """已有道路距离不调用 API；缺少距离且没有密钥时必须报错，不能填假距离。
        Use cached distances without API calls; fail on missing distances without a key rather than inventing values."""
        import json
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "cache.json"
            cache.write_text(json.dumps({"cities": {"RED DEER": [148.5, 131.2]}}), encoding="utf-8")
            with patch.object(distances, "resource_dir", return_value=Path(directory)), patch.object(distances, "api_key", return_value=""), patch.object(distances.requests, "Session") as request:
                self.assertEqual(distances.ensure_distances(["RED DEER"], cache), {"RED DEER": [148.5, 131.2]})
                request.assert_not_called()
                with self.assertRaisesRegex(ValueError, "ORS_API_KEY"):
                    distances.ensure_distances(["EDMONTON"], cache)

    @unittest.skipUnless(REFERENCE.with_name("Brain_Audit.ipynb").is_file(), "原版 Notebook 不在此机器上")
    def test_notebook_model_prediction_parity(self):
        """直接构造 Notebook 原版管线，验证参数、特征顺序及相同轮数的逐行预测一致。
        Construct the original Notebook pipeline and compare parameters, feature order and equal-iteration predictions."""
        import json
        import numpy as np
        from catboost import CatBoostRegressor
        from sklearn.base import BaseEstimator, RegressorMixin
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.pipeline import Pipeline
        from settings import FEATURES, NUM_FEATURES, CAT_FEATURES
        from train import make_model
        notebook = json.loads(REFERENCE.with_name("Brain_Audit.ipynb").read_text(encoding="utf-8"))
        namespace = {"CatBoostRegressor": CatBoostRegressor, "BaseEstimator": BaseEstimator,
                     "RegressorMixin": RegressorMixin, "ColumnTransformer": ColumnTransformer,
                     "SimpleImputer": SimpleImputer, "Pipeline": Pipeline, "RANDOM_SEED": 42}
        names = {"NUM_FEATURES", "CAT_FEATURES", "ALL_FEATURES", "CAT_INDICES", "CATBOOST_PARAMS"}
        functions = {"CatBoostWrapper", "_base_preprocessor", "build_catboost_pipeline"}
        nodes = []
        for cell in notebook["cells"]:
            if cell["cell_type"] != "code":
                continue
            for node in ast.parse("".join(cell["source"])).body:
                if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in names for target in node.targets):
                    nodes.append(node)
                elif isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in functions:
                    nodes.append(node)
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "original_notebook", "exec"), namespace)
        self.assertEqual(FEATURES, namespace["ALL_FEATURES"])
        model = make_model(2000)
        for key, value in namespace["CATBOOST_PARAMS"].items():
            self.assertEqual(model.get_params()[key], value)
        namespace["CATBOOST_PARAMS"] = {**namespace["CATBOOST_PARAMS"], "iterations": 20}
        reference = namespace["build_catboost_pipeline"]()
        rng = np.random.default_rng(42)
        sample = pd.DataFrame({column: rng.uniform(1, 1000, 120) for column in NUM_FEATURES})
        for column in CAT_FEATURES:
            sample[column] = rng.choice(["A", "B", "UNKNOWN"], size=120)
        target = sample["Year"] * 12 + sample["Kilometres"] * 3
        reference.fit(sample[FEATURES], target)
        actual = make_model(20)
        actual.fit(sample[FEATURES], target)
        np.testing.assert_allclose(actual.predict(sample[FEATURES]), reference.predict(sample[FEATURES]), rtol=1e-10, atol=1e-8)
        self.assertEqual(actual.tree_count_, 20)

    def test_road_distance_conversion(self):
        """检查同坐标返回零、经纬度顺序，以及 ORS 米到公里的两位小数转换。
        Check identical-coordinate zero distances, coordinate order and metre-to-kilometre rounding."""
        session = Mock()
        response = session.post.return_value
        response.status_code = 200
        response.json.return_value = {"routes": [{"summary": {"distance": 12345.6}}]}
        origin, target = (-113.5, 53.5), (-114.0, 51.0)
        self.assertEqual(distances.road_distance(origin, origin, "test-key", session), 0)
        session.post.assert_not_called()
        with patch.object(distances.time, "sleep"):
            self.assertEqual(distances.road_distance(origin, target, "test-key", session), 12.35)
        self.assertEqual(session.post.call_args.kwargs["json"], {"coordinates": [origin, target]})

    def test_route_snapping_retry(self):
        """道路匹配超出默认半径时仅重试真实路线请求，其他错误不擅自扩大搜索。
        Retry real routes only for road-matching radius errors; do not broaden searches for unrelated failures."""
        session = Mock()
        missing, success = Mock(), Mock()
        missing.status_code = 404
        missing.json.return_value = {"error": {"code": 2010}}
        success.status_code = 200
        success.json.return_value = {"routes": [{"summary": {"distance": 12500}}]}
        session.post.side_effect = [missing, success]
        with patch.object(distances.time, "sleep"):
            self.assertEqual(distances.road_distance((-113.5, 53.5), (-114.75, 52.08), "test-key", session), 12.5)
        self.assertEqual(session.post.call_count, 2)
        self.assertEqual(session.post.call_args.kwargs["json"]["radiuses"], [1000, 1000])

    def test_geocoding_stays_in_alberta(self):
        """同名城市优先使用 Alberta 坐标，缺失时禁止静默使用其他省份的地点。
        Prefer Alberta coordinates and fail rather than silently using another province."""
        session = Mock()
        wrong = {"name": "Beaumont", "admin1": "Quebec", "country_code": "CA", "longitude": -71.02, "latitude": 46.82}
        right = {"name": "Beaumont", "admin1": "Alberta", "country_code": "CA", "longitude": -113.41871, "latitude": 53.35013}
        session.get.return_value.json.return_value = {"results": [wrong, right]}
        self.assertEqual(distances.get_lat_lon("Beaumont", session), (-113.41871, 53.35013))
        self.assertEqual(session.get.call_args.kwargs["params"]["countryCode"], "CA")
        session.get.return_value.json.return_value = {"results": [wrong]}
        with self.assertRaises(ValueError):
            distances.get_lat_lon("Beaumont", session)

    @unittest.skipUnless(REFERENCE.is_file(), "原版源码不在此机器上")
    def test_original_city_aliases(self):
        """逐项比较道路距离的城市别名表，保证与原版映射完全一致。
        Compare all city aliases with the original mapping."""
        for node in ast.parse(REFERENCE.read_text(encoding="utf-8-sig")).body:
            if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "_CITY_ALIASES" for target in node.targets):
                self.assertEqual(distances.ALIASES, ast.literal_eval(node.value))
                return
        self.fail("未找到原版别名表")

    def test_required_function_comments(self):
        """检查交付源码的每个函数都有真实中文注释，避免编码损坏成问号。
        Verify genuine Chinese function documentation rather than encoding corruption."""
        for path in Path(__file__).parent.glob("*.py"):
            if path.name.startswith("_"):
                continue
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8-sig"))):
                if isinstance(node, ast.FunctionDef):
                    comment = ast.get_docstring(node) or ""
                    self.assertRegex(comment, r"[\u4e00-\u9fff]", f"{path.name}:{node.name}")


if __name__ == "__main__":
    unittest.main()
