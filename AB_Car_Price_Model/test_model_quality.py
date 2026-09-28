import ast
from pathlib import Path
import unittest
import numpy as np
from model_quality import band_index, band_metrics, quality_report, price_range
from train import train_from_csv


class QualityTests(unittest.TestCase):
    """检查区间校准隔离、分档边界、固定轮数及函数注释。
    Check calibration isolation, band boundaries, fixed iterations and function documentation."""

    def test_validation_labels_do_not_fit_intervals(self):
        """扰动验证标签不改变残差分位数，证明没有用验证标签拟合区间。
        Perturb validation labels and verify unchanged quantiles, proving they do not fit the intervals."""
        predicted = np.linspace(1000,100000,1200)
        actual = predicted + np.random.default_rng(42).normal(0,1000,1200)
        quality, audit = quality_report(actual,predicted)
        changed = actual.copy()
        changed[audit.Interval_Split.eq('validation')] += 50000
        second, other = quality_report(changed,predicted)
        self.assertEqual(quality['prediction_intervals']['bands'],second['prediction_intervals']['bands'])
        self.assertNotEqual(quality['prediction_intervals']['validation_coverage'],second['prediction_intervals']['validation_coverage'])
        self.assertEqual(set(audit.Interval_Split),{'calibration','validation'})

    def test_boundaries_and_metrics(self):
        """检查边界归属和实际价格分档误差，空档不产生虚构分数。
        Check band boundaries and errors; never invent scores for empty bands."""
        self.assertEqual([band_index(x) for x in [-1,0,2999,3000,75000]],[0,0,0,1,14])
        rows = band_metrics([1000,3000],[2000,4000])
        self.assertEqual(rows[0]['N'],1)
        self.assertEqual(rows[1]['N'],1)
        self.assertEqual(rows[-1]['MAE'],1000)
        self.assertEqual(rows[-1]['Relative MAE'],0.5)
        self.assertIsNone(rows[2]['R²'])

    def test_ranges_and_fallback(self):
        """稀疏价档回退整体残差，区间非负并向外取整。
        Use pooled residuals for sparse bands, with nonnegative bounds and outward rounding."""
        quality, audit = quality_report(np.arange(100)+1000,np.arange(100)+1200)
        ref = quality['prediction_intervals']
        result = price_range(1234,ref)
        self.assertEqual((result['lower_cad'],result['upper_cad']),(1000,1100))
        self.assertTrue(result['pooled_fallback'])
        low = price_range(-999,ref)
        self.assertEqual(low['lower_cad'],0)
        self.assertGreater(low['upper_cad'],low['lower_cad'])
        with self.assertRaises(ValueError):
            price_range(float('nan'),ref)

    def test_fixed_iterations_before_io(self):
        """错误轮数应在读取文件和写入数据库之前拒绝。
        Reject invalid iteration counts before file reads or database writes."""
        with self.assertRaises(ValueError):
            train_from_csv('does-not-exist.csv','unused',iterations=1999)

    def test_function_docstrings(self):
        """检查本次修改文件的每段函数都有说明注释。
        Check documentation on every function in the modified files."""
        for filename in ['model_quality.py','publish_selected_model.py','train.py','predict.py','app.py', 'test_model_quality.py']:
            for node in ast.walk(ast.parse(Path(filename).read_text(encoding='utf-8-sig'))):
                if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)):
                    self.assertTrue(ast.get_docstring(node), f'{filename}:{node.name}')


if __name__ == '__main__':
    unittest.main()
