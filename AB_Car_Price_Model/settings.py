from pathlib import Path
import sys

# 训练与预测共用同一特征顺序，避免模型输入错位。
# Use identical feature order for training and prediction to avoid misaligned inputs.
NUM_FEATURES = ["Year", "Kilometres", "Distance_from_Edmonton_KM", "Distance_from_Calgary_KM"]
CAT_FEATURES = ["Condition_Label", "Transmission_Type", "Drivetrain_Type",
                "Body_Style", "Colour", "Seats_Count", "City_Name", "Base_Model", "Trim"]
FEATURES = NUM_FEATURES + CAT_FEATURES
DATABASE = "AB_Car_Price_Model"


def resource_dir():
    """取得源码目录或 PyInstaller 解包后的只读资源目录。
    Return the source directory or PyInstaller's extracted read-only resource directory."""
    return Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))


def application_dir():
    """取得程序所在目录，用于寻找外置模型及保存训练结果。
    Return the application directory for external models and training outputs."""
    return Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
