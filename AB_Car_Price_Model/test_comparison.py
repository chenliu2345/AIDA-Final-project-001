import unittest
import numpy as np
import pandas as pd
from compare_training import define_splits


class ComparisonTests(unittest.TestCase):
    """检查四套划分覆盖完整样本，且验证或测试行不会参与该模型拟合。
    Check complete split coverage and ensure validation and test rows never participate in fitting."""

    def test_four_splits_and_common_test(self):
        """验证分组隔离、随机方案共用外层测试集，以及共同测试集对四个方案均未见。
        Check group isolation, the shared random outer test set and the common test set being unseen by all four evaluation models."""
        frame = pd.DataFrame({'Group_Key': np.repeat(np.arange(1000), 5)})
        splits, common = define_splits(frame)
        self.assertEqual(len(splits), 4)
        self.assertGreater(len(common), 1)
        for name, (train, validation, test) in splits.items():
            self.assertEqual(sorted(list(train)+list(validation)+list(test)), list(range(len(frame))))
            self.assertFalse(set(train) & set(validation) or set(train) & set(test) or set(validation) & set(test))
            self.assertTrue(set(common).issubset(test))
            if name in {'my_scheme', 'grouped_70_15_15_es'}:
                groups = [set(frame.iloc[indices].Group_Key) for indices in [train, validation, test]]
                self.assertFalse(groups[0] & groups[1] or groups[0] & groups[2] or groups[1] & groups[2])
        np.testing.assert_array_equal(splits['original_scheme'][2], splits['random_80_20_es'][2])
        self.assertEqual(tuple(map(len,splits['random_80_20_es'])), (3200,800,1000))
        self.assertEqual(tuple(map(len,splits['grouped_70_15_15_es'])), (3500,750,750))


if __name__ == '__main__':
    unittest.main()
