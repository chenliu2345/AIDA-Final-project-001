import argparse
import json
from pathlib import Path
import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import traceback

from predict import load_model, predict_price
from settings import CAT_FEATURES, application_dir

LABELS = {"Year": "年份", "Kilometres": "里程（公里）", "Base_Model": "品牌 / 车型",
          "Trim": "配置", "City_Name": "城市", "Condition_Label": "车况",
          "Transmission_Type": "变速箱", "Drivetrain_Type": "驱动方式",
          "Body_Style": "车身类型", "Colour": "颜色", "Seats_Count": "座位数"}


class PriceApplication:
    """提供误差报告、离线预测和一键训练三个页面，将耗时训练放在后台线程。
    Provide error-report, offline valuation and CSV training pages, with training on a background thread."""

    def __init__(self, root):
        """创建窗口、共享状态与界面，并尝试载入已发布或内置模型。
        Create the window, shared state and interface, then load the published or bundled model."""
        self.root = root
        self.model = self.metadata = None
        self.events = queue.Queue()
        self.busy = False
        self.fields = {}
        self.widgets = {}
        root.title("AB Car Price · 阿尔伯塔二手车估价")
        root.geometry("1040x780")
        root.minsize(960, 780)
        root.configure(bg="#f4f6fa")
        root.protocol("WM_DELETE_WINDOW", self.close)
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background="#f4f6fa")
        style.configure("TLabel", background="#f4f6fa", font=("Microsoft YaHei UI", 10))
        style.configure("Title.TLabel", font=("Microsoft YaHei UI", 21, "bold"), foreground="#16324f")
        style.configure("Price.TLabel", font=("Microsoft YaHei UI", 20, "bold"), foreground="#126b58")
        style.configure("TButton", font=("Microsoft YaHei UI", 10), padding=8)
        style.configure("TNotebook.Tab", font=("Microsoft YaHei UI", 10), padding=(18, 9))
        outer = ttk.Frame(root, padding=22)
        outer.pack(fill="both", expand=True)
        ttk.Label(outer, text="AB Car Price", style="Title.TLabel").pack(anchor="w")
        ttk.Label(outer, text="基于阿尔伯塔车辆挂牌数据的离线价格参考", foreground="#64748b").pack(anchor="w", pady=(4, 16))
        notebook = self.notebook = ttk.Notebook(outer)
        notebook.pack(fill="both", expand=True)
        errors = ttk.Frame(notebook, padding=18)
        notebook.add(errors, text="模型误差")
        prediction = self.prediction_page = ttk.Frame(notebook, padding=18)
        training = ttk.Frame(notebook, padding=18)
        notebook.add(prediction, text="车辆估价")
        notebook.add(training, text="从 CSV 训练模型")
        self.build_error_page(errors)
        self.build_prediction_page(prediction)
        self.build_training_page(training)
        self.load_current_model()
        self.root.after(150, self.poll_events)

    def build_error_page(self, parent):
        """将总体和实际价格分档误差放在启动首页，并展示区间覆盖率的验证口径。
        Show overall and price-band errors on the opening page and explain interval coverage."""
        self.error_summary = ttk.Label(parent, text="正在加载误差报告…", wraplength=910)
        self.error_summary.pack(anchor="w", pady=(0, 10))
        columns = ("Actual Price", "N", "MAE", "MedAE", "R²", "Relative MAE")
        table_frame = ttk.Frame(parent)
        table_frame.pack(fill="both", expand=True)
        self.error_table = ttk.Treeview(table_frame, columns=columns, show="headings", height=6)
        for column in columns:
            self.error_table.heading(column, text=column)
            self.error_table.column(column, width=135, anchor="center")
        scrollbar = ttk.Scrollbar(table_frame, command=self.error_table.yview)
        self.error_table.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        self.error_table.pack(side="left", fill="both", expand=True)
        self.coverage_label = ttk.Label(parent, text="", wraplength=910, foreground="#475569")
        self.coverage_label.pack(anchor="w", pady=(10, 5))
        ttk.Label(parent, text="价格单位 CAD。Relative MAE = MAE / 平均实际价格。窄价档的 R² 可能为负，请结合 MAE、MedAE 和样本数判断。", wraplength=910, foreground="#64748b").pack(anchor="w")
        ttk.Button(parent, text="进入车辆估价 →", command=lambda: self.notebook.select(self.prediction_page)).pack(anchor="e", pady=(8, 0))

    def refresh_error_page(self):
        """随模型切换同步刷新误差表，避免显示其他模型的评估结果。
        Refresh metrics when the loaded model changes."""
        meta = self.metadata
        metrics = meta["test_metrics"]
        counts = meta["split_counts"]
        self.error_summary.configure(text=f"Random 80/20 · 固定 2,000 轮 · 不提前停止\n评估训练 {counts['train']:,} 条 / 留出测试 {counts['test']:,} 条；部署模型使用全部 {meta['training_rows']:,} 条 SOLD。\n留出测试 MAE ${metrics['mae_cad']:,.0f} · R² {metrics['r2']:.3f} · RMSE ${metrics['rmse_cad']:,.0f}")
        self.error_table.delete(*self.error_table.get_children())
        for row in meta["price_band_metrics"]:
            values = [row["Actual Price"], f"{row['N']:,}"]
            for key in ["MAE", "MedAE", "R²", "Relative MAE"]:
                value = row[key]
                values.append("—" if value is None else f"{value:.1%}" if key == "Relative MAE" else f"{value:.3f}" if key == "R²" else f"${value:,.0f}")
            self.error_table.insert("", "end", values=values)
        ref = meta["prediction_intervals"]
        self.coverage_label.configure(text=f"价格范围：预测价档历史残差的 10%–90% 分位区间；校准 {ref['calibration_n']:,} 条，另行验证 {ref['validation_n']:,} 条，覆盖率 {ref['validation_coverage']:.1%}。\n覆盖率来自留出评估模型；全量部署模型的区间仅作参考。随机行划分可能让同车记录跨集合，指标不等同于全新车辆泛化表现。")

    def build_prediction_page(self, parent):
        """创建车辆参数表单、模型信息和价格输出；类别选项从真实训练数据生成。
        Build vehicle inputs and price outputs using real training categories."""
        self.model_label = ttk.Label(parent, text="正在加载模型…", wraplength=910)
        self.model_label.pack(anchor="w", pady=(0, 12))
        form = ttk.Frame(parent)
        form.pack(fill="x")
        form.columnconfigure(1, weight=1)
        form.columnconfigure(3, weight=1)
        order = ["Base_Model", "Trim", "Year", "Kilometres", "City_Name", "Condition_Label",
                 "Transmission_Type", "Drivetrain_Type", "Body_Style", "Colour", "Seats_Count"]
        for index, column in enumerate(order):
            row, position = divmod(index, 2)
            ttk.Label(form, text=LABELS[column]).grid(row=row, column=position*2, sticky="w", padx=(0, 10), pady=6)
            value = tk.StringVar(value="2019" if column == "Year" else "65000" if column == "Kilometres" else "")
            widget = ttk.Combobox(form, textvariable=value, state="readonly", width=28) if column in CAT_FEATURES else ttk.Entry(form, textvariable=value, width=28)
            widget.grid(row=row, column=position*2+1, sticky="ew", padx=(0, 20), pady=6)
            self.fields[column], self.widgets[column] = value, widget
        self.widgets["Base_Model"].bind("<<ComboboxSelected>>", self.update_trims)
        actions = ttk.Frame(parent)
        actions.pack(fill="x", pady=16)
        self.predict_button = ttk.Button(actions, text="计算挂牌价格范围", command=self.predict)
        self.predict_button.pack(side="left")
        ttk.Button(actions, text="选择模型目录", command=self.choose_model).pack(side="left", padx=12)
        results = ttk.Frame(parent)
        results.pack(fill="x", pady=(0, 8))
        results.columnconfigure(0, weight=3)
        results.columnconfigure(1, weight=2)
        ttk.Label(results, text="历史误差参考范围").grid(row=0, column=0, sticky="w")
        ttk.Label(results, text="模型预测价格").grid(row=0, column=1, sticky="w", padx=(16, 0))
        self.price_label = ttk.Label(results, text="— CAD", style="Price.TLabel")
        self.price_label.grid(row=1, column=0, sticky="w", pady=(4, 0))
        self.point_price_label = ttk.Label(results, text="— CAD", style="Price.TLabel")
        self.point_price_label.grid(row=1, column=1, sticky="w", padx=(16, 0), pady=(4, 0))
        self.detail_label = ttk.Label(parent, text="填写车辆信息后计算。", wraplength=900, foreground="#475569")
        self.detail_label.pack(anchor="w")
        ttk.Label(parent, text="结果反映挂牌价格规律；不代表已核实成交价，也不保证销售速度。", foreground="#64748b", wraplength=900).pack(anchor="w", pady=(16, 0))

    def build_training_page(self, parent):
        """创建原始 CSV、SQL 实例和输出目录选项，并显示后台训练进度。
        Build CSV, SQL instance and output-folder inputs with training progress."""
        ttk.Label(parent, text="选择原始爬虫 CSV，一次完成 ETL、SQL 入库、Random 80/20 固定 2000 轮评估和全量训练。", wraplength=900).pack(anchor="w", pady=(0, 8))
        ttk.Label(parent, text="训练需要 SQL Server 和 ODBC 17/18。新城市的道路距离需要本地 .env 中的 ORS_API_KEY；已缓存城市可离线估价。", wraplength=900, foreground="#64748b").pack(anchor="w", pady=(0, 16))
        self.csv_path = tk.StringVar()
        self.server = tk.StringVar(value=".")
        self.output_path = tk.StringVar(value=str(application_dir() / "models"))
        self.training_controls = []
        for title, variable, command in [("原始 CSV", self.csv_path, self.choose_csv),
                                          ("SQL Server", self.server, None),
                                          ("模型输出目录", self.output_path, self.choose_output)]:
            line = ttk.Frame(parent)
            line.pack(fill="x", pady=5)
            ttk.Label(line, text=title, width=15).pack(side="left")
            entry = ttk.Entry(line, textvariable=variable)
            entry.pack(side="left", fill="x", expand=True)
            self.training_controls.append(entry)
            if command:
                button = ttk.Button(line, text="浏览", command=command)
                button.pack(side="left", padx=(10, 0))
                self.training_controls.append(button)
        self.train_button = ttk.Button(parent, text="开始完整训练", command=self.start_training)
        self.train_button.pack(anchor="w", pady=12)
        self.progress = ttk.Progressbar(parent, mode="indeterminate")
        self.progress.pack(fill="x", pady=(0, 12))
        log_frame = ttk.Frame(parent)
        log_frame.pack(fill="both", expand=True)
        scrollbar = ttk.Scrollbar(log_frame)
        scrollbar.pack(side="right", fill="y")
        self.log_widget = tk.Text(log_frame, height=12, wrap="word", state="disabled", font=("Microsoft YaHei UI", 10), bg="#ffffff", relief="flat", yscrollcommand=scrollbar.set)
        self.log_widget.pack(side="left", fill="both", expand=True)
        scrollbar.config(command=self.log_widget.yview)

    def load_current_model(self, directory=None):
        """加载模型并刷新下拉选项；无模型时仍允许用户进入训练页面。
        Load the model and refresh choices; allow training when no model exists."""
        try:
            model, metadata, path = load_model(directory)
        except Exception as error:
            if self.model is None:
                self.model_label.configure(text=str(error))
                self.error_summary.configure(text=str(error))
                self.predict_button.configure(state="disabled")
            else:
                messagebox.showerror("加载失败", str(error))
            return
        self.model, self.metadata = model, metadata
        self.refresh_error_page()
        self.predict_button.configure(state="normal")
        metrics = metadata["test_metrics"]
        self.model_label.configure(text=f"最终版 ETL · {metadata['training_rows']:,} 条 SOLD 训练记录 · 独立测试 MAE ${metrics['mae_cad']:,.0f} CAD · R² {metrics['r2']:.3f}")
        defaults = {"Base_Model": "TOYOTA RAV4", "Trim": "XLE", "City_Name": "RED DEER", "Condition_Label": "USED", "Transmission_Type": "AUTOMATIC", "Drivetrain_Type": "ALL-WHEEL DRIVE (AWD)", "Body_Style": "SUV, CROSSOVER", "Colour": "WHITE EXTERIOR", "Seats_Count": "5 SEATS"}
        for column in CAT_FEATURES:
            options = metadata["catalog"][column]
            self.widgets[column].configure(values=options)
            self.fields[column].set(defaults.get(column) if defaults.get(column) in options else options[0])
        self.update_trims()
        self.price_label.configure(text="— CAD")
        self.point_price_label.configure(text="— CAD")
        self.detail_label.configure(text="填写车辆信息后计算。测试误差是整体评估结果，不是单辆车的置信区间。")

    def update_trims(self, event=None):
        """按当前车型筛选配置，避免界面组合出明显无关的车型和配置。
        Filter trims by model to avoid unrelated combinations."""
        if self.metadata:
            options = self.metadata["trims_by_model"].get(self.fields["Base_Model"].get(), ["UNKNOWN"])
            self.widgets["Trim"].configure(values=options)
            if self.fields["Trim"].get() not in options:
                self.fields["Trim"].set("UNKNOWN" if "UNKNOWN" in options else options[0])

    def predict(self, show_errors=True):
        """读取表单并显示预测结果，输入错误直接展示给用户。
        Read the form and display predictions or input errors."""
        try:
            result = predict_price(self.model, self.metadata, {key: value.get() for key, value in self.fields.items()})
            self.price_label.configure(text=f"${result['lower_cad']:,.0f} – ${result['upper_cad']:,.0f} CAD")
            self.point_price_label.configure(text=f"${result['listing_price_cad']:,.0f} CAD")
            self.detail_label.configure(text="历史误差参考区间，并非单车成交价保证。请结合实车状况和同类挂牌定价。\n" + "\n".join(result["warnings"]))
            return result
        except Exception as error:
            if not show_errors:
                raise
            messagebox.showerror("无法估价", str(error))
            return None

    def choose_model(self):
        """允许选择训练输出根目录或其中某个版本目录。
        Select a model root or an individual version folder."""
        path = filedialog.askdirectory(title="选择模型目录")
        if path:
            self.load_current_model(path)

    def choose_csv(self):
        """打开文件选择器，只将用户选择的 CSV 作为训练输入。
        Use only the user-selected CSV as training input."""
        path = filedialog.askopenfilename(title="选择原始爬虫 CSV", filetypes=[("CSV 文件", "*.csv")])
        if path:
            self.csv_path.set(path)

    def choose_output(self):
        """选择有写入权限的模型保存目录，便于 EXE 放在只读位置时使用。
        Choose a writable output folder when the EXE directory is read-only."""
        path = filedialog.askdirectory(title="选择模型输出目录")
        if path:
            self.output_path.set(path)

    def start_training(self):
        """验证输入并启动后台训练，期间禁用训练参数以保持本次任务一致。
        Validate inputs and lock training settings during the background run."""
        if self.busy:
            return
        if not Path(self.csv_path.get()).is_file():
            messagebox.showerror("缺少 CSV", "请先选择原始 CSV 文件。")
            return
        self.busy = True
        for control in self.training_controls + [self.train_button]:
            control.configure(state="disabled")
        self.progress.start(12)
        threading.Thread(target=self.training_worker, args=(self.csv_path.get(), self.output_path.get(), self.server.get()), daemon=True).start()

    def training_worker(self, csv_path, output_path, server):
        """在后台执行训练，仅通过线程安全队列传递消息，不直接操作 Tk 控件。
        Run training in the background; communicate through a thread-safe queue without touching Tk widgets."""
        try:
            from train import train_from_csv
            train_from_csv(csv_path, output_path, server, log=self.queue_log)
            self.events.put(("done", output_path))
        except Exception:
            self.events.put(("error", traceback.format_exc()))

    def queue_log(self, message):
        """将后台训练日志加入队列，交给主线程更新界面。
        Queue training messages for the main thread."""
        self.events.put(("log", message))

    def poll_events(self):
        """定期消费后台事件，更新日志、进度条和训练完成后的预测模型。
        Consume events, update progress and load the completed model."""
        while True:
            try:
                kind, message = self.events.get_nowait()
            except queue.Empty:
                break
            self.log_widget.configure(state="normal")
            self.log_widget.insert("end", message + "\n")
            self.log_widget.see("end")
            self.log_widget.configure(state="disabled")
            if kind in {"done", "error"}:
                self.busy = False
                self.progress.stop()
                for control in self.training_controls + [self.train_button]:
                    control.configure(state="normal")
                if kind == "done":
                    self.load_current_model(message)
                    messagebox.showinfo("训练完成", "新模型已加载，可以直接在车辆估价页使用。")
                else:
                    messagebox.showerror("训练失败", message.splitlines()[-1])
        self.root.after(150, self.poll_events)

    def close(self):
        """避免误关窗口打断训练；空闲时直接退出程序。
        Prevent accidental closure during training; exit directly when idle."""
        if self.busy:
            messagebox.showinfo("训练进行中", "请等待本次训练结束后再关闭窗口。")
            return
        self.root.destroy()


def self_test(output_path):
    """验证模型、离线预测及 Tk 表单，供打包后的 EXE 自动验收使用。
    Validate the model, offline predictions and Tk form in the packaged EXE."""
    Path(output_path).resolve().parent.mkdir(parents=True, exist_ok=True)
    root = None
    try:
        model, metadata, directory = load_model()
        values = {column: metadata["catalog"][column][0] for column in CAT_FEATURES}
        values.update(Year=2019, Kilometres=65000)
        result = predict_price(model, metadata, values)
        root = tk.Tk()
        root.withdraw()
        app = PriceApplication(root)
        root.update_idletasks()
        gui_result = app.predict(show_errors=False)
        if not gui_result or app.model is None:
            raise RuntimeError("界面预测未成功。")
        assert app.notebook.index(app.notebook.select()) == 0
        assert len(app.error_table.get_children()) == 16
        assert model.tree_count_ == metadata["best_iterations"] == 2000
        assert 0 <= gui_result["lower_cad"] < gui_result["upper_cad"]
        assert app.point_price_label.cget("text") == f"${gui_result['listing_price_cad']:,.0f} CAD"
        import pyodbc
        from train import make_model
        make_model(20)
        payload = {"ok": True, "model_directory": str(directory), "prediction": result, "gui_prediction": gui_result, "odbc_drivers": pyodbc.drivers(), "training_rows": metadata["training_rows"], "model_sha256": metadata["model_sha256"], "iterations": model.tree_count_, "first_tab": app.notebook.tab(0, "text"), "error_rows": len(app.error_table.get_children()), "displayed_range": app.price_label.cget("text"), "displayed_prediction": app.point_price_label.cget("text")}
        Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        Path(output_path).write_text(json.dumps({"ok": False, "error": traceback.format_exc()}, ensure_ascii=False), encoding="utf-8")
        raise
    finally:
        if root:
            root.destroy()


def main():
    """启动桌面程序，同时支持 EXE 自检与无人值守训练命令。
    Start the desktop app, self-test or unattended training command."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", metavar="RESULT_JSON")
    parser.add_argument("--train", metavar="RAW_CSV")
    parser.add_argument("--output", default=str(application_dir() / "models"))
    parser.add_argument("--server", default=".")
    parser.add_argument("--iterations", type=int, default=2000)
    args = parser.parse_args()
    if args.self_test:
        self_test(args.self_test)
    elif args.train:
        from train import train_from_csv
        output = Path(args.output)
        output.mkdir(parents=True, exist_ok=True)
        with (output / "training.log").open("w", encoding="utf-8", buffering=1) as stream:
            try:
                train_from_csv(args.train, output, args.server, args.iterations, log=lambda message: stream.write(message + "\n"))
            except Exception:
                stream.write(traceback.format_exc())
                raise
    else:
        root = tk.Tk()
        PriceApplication(root)
        root.mainloop()


if __name__ == "__main__":
    main()
