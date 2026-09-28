from types import SimpleNamespace
import unittest
import numpy as np
import pandas as pd
from price_anomaly import decide, severity, comparable_stats, keyword_hits


class AnomalyRulesTest(unittest.TestCase):
    """验证自动排除、特殊车辆保护、边界和可比数据隔离。
    Verify exclusion rules, special-vehicle protection, thresholds and comparable-data isolation."""

    def record(self, price=330000, title='2019 Honda Pilot Touring', model='Honda Pilot', trim='Touring'):
        """构造只含决策需要字段的车辆记录。
        Build a vehicle record with only the required decision fields."""
        return SimpleNamespace(Price_CAD=price,Listing_Title=title,Base_Model=model,Trim=trim)

    def comparable(self, median=32000, count=20, matched=True):
        """构造稳定且可独立调整置信度的可比车辆证据。
        Build stable comparable evidence with independently adjustable confidence."""
        return {'Comparable_Count':count,'Comparable_Median':median,'Comparable_P25':median*.9,'Comparable_P75':median*1.1,'Comparable_Exact_Trim_Body_Drive':matched}

    def test_extra_zero_requires_both(self):
        """单一模型异常不得删除，模型和可比必须同时偏离才可排除。
        Require both model and comparable deviations; a model anomaly alone cannot exclude a row."""
        self.assertEqual(decide(self.record(),29000,self.comparable())['Decision'],'AUTO_EXCLUDE')
        self.assertEqual(decide(self.record(),29000,self.comparable(300000))['Decision'],'REVIEW')

    def test_missing_zero(self):
        """少一个零只能排除，原价格必须保留。
        Exclude a missing-zero case without changing its original price."""
        result=decide(self.record(3300),30000,self.comparable())
        self.assertEqual(result['Decision'],'AUTO_EXCLUDE')
        self.assertEqual(result['Actual_Price'],3300)
        self.assertTrue(result['Possible_Missing_Zero'])

    def test_low_comparable_blocks(self):
        """少于五个可比车辆组时禁止自动排除。
        Block exclusion with fewer than five independent comparable groups."""
        self.assertEqual(decide(self.record(),29000,self.comparable(count=4))['Decision'],'REVIEW')

    def test_special_protection(self):
        """特殊车型缺乏匹配配置证据时必须保留复核。
        Keep special vehicles for review when matching trim evidence is insufficient."""
        row=self.record(title='Porsche 911 GT3',model='Porsche 911',trim='GT3')
        self.assertEqual(decide(row,29000,self.comparable(matched=False))['Decision'],'REVIEW')
        self.assertEqual(decide(row,29000,self.comparable(count=2))['Decision'],'REVIEW')

    def test_payment(self):
        """付款标题必须同时得到模型与可比低价证据支持。
        Require model and comparable low-price evidence for payment-related titles."""
        row=self.record(1299,'2026 Mercedes GLE450 Lease Takeover','Mercedes GLE','450')
        self.assertEqual(decide(row,71000,self.comparable(70000))['Decision'],'AUTO_EXCLUDE')
        self.assertEqual(decide(row,71000,self.comparable(1400))['Decision'],'REVIEW')

    def test_parts_not_automatic(self):
        """项目车标题自身不触发排除，否定和无事故短语不算异常。
        Project-car titles alone cannot exclude rows; negations and accident-free phrases are not anomalies."""
        self.assertEqual(decide(self.record(30000,'Honda Pilot project car'),29000,self.comparable())['Decision'],'REVIEW')
        self.assertEqual(keyword_hits('no collision damage; collision-free; new parts',['collision','parts']),[])

    def test_boundaries(self):
        """核对模型等级边界，避免等号误判。
        Check anomaly boundaries to avoid equality mistakes."""
        self.assertEqual(severity(.4)[0],'NORMAL')
        self.assertEqual(severity(2.5)[0],'NORMAL')
        self.assertEqual(severity(.25)[0],'SUSPICIOUS')
        self.assertEqual(severity(4)[0],'SUSPICIOUS')
        self.assertEqual(severity(8)[0],'HIGHLY_SUSPICIOUS')

    def test_comparable_reference(self):
        """可比数据不含自身、同组或测试价格，按独立组数选择层级。
        Exclude self, same-group and test prices; select comparable levels by independent group counts."""
        rows=[dict(Base_Model='Pilot',Group_Key=str(i),Year=2019,Kilometres=100000,Trim='Touring',Body_Style='SUV',Drivetrain_Type='AWD',Price_CAD=30000) for i in range(7)]
        rows[6]['Price_CAD']=999999
        rows.append(dict(rows[0],Price_CAD=888888))
        frame=pd.DataFrame(rows)
        result=comparable_stats(frame,np.arange(6))
        self.assertEqual(result.iloc[0].Comparable_Count,5)
        self.assertEqual(result.iloc[6].Comparable_Median,30000)
        self.assertEqual(result.iloc[7].Comparable_Count,5)

    def test_unknown_base_not_comparable(self):
        """未知车型不能因为同为 OTHER 而互相充当可比车辆。
        Do not treat unknown models as comparable just because both are OTHER."""
        frame=pd.DataFrame([dict(Base_Model='OTHER',Group_Key=str(i),Year=2020,Kilometres=10000,Trim='OTHER',Body_Style='SUV',Drivetrain_Type='AWD',Price_CAD=30000) for i in range(7)])
        result=comparable_stats(frame,np.arange(6))
        self.assertTrue(result.Comparable_Count.eq(0).all())

    def test_case_a_matches_written_rule(self):
        """两种价格参考均偏离四倍且其中一个支持十倍，符合用户原文 Case A。
        Both references deviating fourfold, with one supporting tenfold, meets the original Case A."""
        self.assertEqual(decide(self.record(),51000,self.comparable())['Decision'],'AUTO_EXCLUDE')
        self.assertEqual(decide(self.record(),100000,self.comparable())['Decision'],'REVIEW')

    def test_dodge_5500_protection(self):
        """Dodge 5500 商用车型不能被错误归类的普通皮卡可比价格排除。
        Do not exclude commercial Dodge 5500 vehicles using misclassified ordinary-pickup comparables."""
        row=self.record(150000,'2013 Dodge 5500 4x4','DODGE POWER RAM 2500','OTHER')
        result=decide(row,17351,self.comparable(13100,matched=False))
        self.assertTrue(result['Rare_or_Special_Vehicle'])
        self.assertEqual(result['Decision'],'REVIEW')

    def test_commercial_modified_and_bundled_protection(self):
        """保护作业卡车、改装限量车和房车组合，不用普通车型证据误删。
        Protect work trucks, modified/limited vehicles and RV combinations from unsuitable ordinary-vehicle evidence."""
        for title in ['2002 GMC 7500 Series 5 Ton Steam Truck','2007 FREIGHTLINER M2 SPORT TRUCK','2007 Peterbilt 335 Hooklift','2009 Jeep SRT8 Fully Built 2000HP','99 Ford Dually and 2015 Artic Fox Camper','2008 FORD F250 BOYD CODDINGTON EDITION #11/50','1997 Ford E350 4x4 Diesel']:
            result=decide(self.record(100000,title),10000,self.comparable(10000,matched=False))
            self.assertTrue(result['Rare_or_Special_Vehicle'],title)
            self.assertEqual(result['Decision'],'REVIEW',title)

    def test_parts_need_dual_extreme(self):
        """项目车即使存在十倍模式，也必须满足双极端偏离才能排除。
        Project cars still require both extreme deviations even with a tenfold pattern."""
        result=decide(self.record(1500,'Honda project car'),15000,self.comparable(10000))
        self.assertEqual(result['Decision'],'REVIEW')

    def test_as_is_and_fault_synonyms(self):
        """明确按现状整车出售和故障同义词也不能仅凭十倍模式删除。
        As-is whole-vehicle listings and fault synonyms cannot be excluded solely by a tenfold pattern."""
        for title in ['BUY AS WHOLE AS IS Honda','Honda BLOWN MOTOR','Honda rusted out','Honda partout']:
            self.assertEqual(decide(self.record(1500,title),15000,self.comparable(10000))['Decision'],'REVIEW')


if __name__ == '__main__':
    unittest.main()
