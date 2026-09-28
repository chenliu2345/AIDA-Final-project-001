import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def build_executable(project_dir, language="zh"):
    """把已验证的部署模型及 SQL 脚本封装进单文件 Windows EXE。
    Bundle the verified deployment model and SQL schema into a single Windows EXE."""
    from predict import load_model
    project_dir = Path(project_dir).resolve()
    executable_name = "AB_Car_Price_EN" if language == "en" else "AB_Car_Price"
    source_dir = project_dir
    if language == "en":
        from build_english import generate_english_sources
        source_dir = generate_english_sources(project_dir)
    model, metadata, version_dir = load_model(project_dir / "models")
    bundle = project_dir / ".packaging" / "bundled_model"
    bundle.mkdir(parents=True, exist_ok=True)
    for name in ["price_model.cbm", "metadata.json"]:
        shutil.copy2(version_dir / name, bundle / name)
    distance_cache = project_dir / "models" / "distance_cache.json"
    command = [sys.executable, "-m", "PyInstaller", "--noconfirm", "--clean", "--onefile", "--windowed",
               "--name", executable_name, "--distpath", str(project_dir / "dist"),
               "--workpath", str(project_dir / ".packaging" / "work"),
               "--specpath", str(project_dir / ".packaging"),
               "--add-data", str(project_dir / "schema.sql") + ";.",
               "--add-data", str(bundle) + ";bundled_model",
               "--add-data", str(distance_cache) + ";.",
               "--collect-data", "catboost", "--hidden-import", "catboost._catboost",
               "--hidden-import", "backports.tarfile"]
    for package in ["matplotlib", "IPython", "notebook", "jupyter", "pytest", "plotly", "bokeh", "torch", "tensorflow"]:
        command.extend(["--exclude-module", package])
    command.append(str(source_dir / "launcher.py"))
    # Conda 衍生的 venv 必须优先解析其基础解释器的 DLL，避免捡到另一套 Conda 的运行库。
    # Prefer the base interpreter DLLs for a Conda-derived venv, avoiding another Conda runtime.
    environment = os.environ.copy()
    library = Path(sys.base_prefix) / "Library" / "bin"
    if library.is_dir():
        environment["PATH"] = str(library) + os.pathsep + str(Path(sys.base_prefix)) + os.pathsep + environment.get("PATH", "")
    subprocess.run(command, cwd=project_dir, env=environment, check=True)
    # 必须在打包产物上验证，而不只在源码环境验证。
    # Verify the packaged executable, not just the source environment.
    report = project_dir / ".packaging" / f"self_test_{language}.json"
    subprocess.run([str(project_dir / "dist" / f"{executable_name}.exe"), "--self-test", str(report)], check=True, timeout=240)
    if not json.loads(report.read_text(encoding="utf-8")).get("ok"):
        raise RuntimeError("EXE 自检失败，请检查 .packaging/self_test.json。")
    print("EXE build and self-test passed:", project_dir / "dist" / f"{executable_name}.exe")


def main():
    """提供一键训练并打包入口；未指定 CSV 时弹出本地文件选择器。
    Train and build in one command; open a CSV picker when no path is supplied."""
    parser = argparse.ArgumentParser(description="原始 CSV → SQL → 模型 → EXE")
    parser.add_argument("--csv")
    parser.add_argument("--server", default=".")
    parser.add_argument("--iterations", type=int, default=2000)
    parser.add_argument("--skip-train", action="store_true")
    parser.add_argument("--language", choices=["zh", "en"], default="zh")
    args = parser.parse_args()
    project_dir = Path(__file__).resolve().parent
    if not args.skip_train:
        csv_path = args.csv
        if not csv_path:
            import tkinter as tk
            from tkinter import filedialog
            root = tk.Tk()
            root.withdraw()
            csv_path = filedialog.askopenfilename(title="选择原始爬虫 CSV", filetypes=[("CSV", "*.csv")])
            root.destroy()
        if not csv_path:
            raise SystemExit("未选择 CSV，操作已取消。")
        from train import train_from_csv
        train_from_csv(csv_path, project_dir / "models", args.server, args.iterations)
    build_executable(project_dir, args.language)


if __name__ == "__main__":
    main()
