import json
import os
from pathlib import Path
import sys
import traceback


def main():
    """为无控制台 EXE 提供日志流，并将启动失败写入自检报告或错误日志。
    Provide streams for a windowed EXE and save startup failures to self-test reports or error logs."""
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w", encoding="utf-8")
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w", encoding="utf-8")
    try:
        from app import main as start_application
        start_application()
    except Exception:
        error = traceback.format_exc()
        if "--self-test" in sys.argv:
            index = sys.argv.index("--self-test")
            Path(sys.argv[index + 1]).write_text(json.dumps({"ok": False, "error": error}, ensure_ascii=False), encoding="utf-8")
            raise SystemExit(1)
        if "--train" in sys.argv:
            output = Path(sys.argv[sys.argv.index("--output") + 1]) if "--output" in sys.argv else Path(sys.executable).parent / "models"
            output.mkdir(parents=True, exist_ok=True)
            with (output / "training.log").open("a", encoding="utf-8") as stream:
                stream.write(error)
            raise SystemExit(1)
        # 正常双击启动时保留 PyInstaller 的可见错误提示，避免静默退出。
        # Keep PyInstaller startup errors visible on normal double-click launches instead of failing silently.
        raise


if __name__ == "__main__":
    main()
