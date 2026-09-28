import ast
import json
from pathlib import Path

MODULES = ['app.py','predict.py','train.py','distances.py','database.py','etl.py','model_quality.py','settings.py','launcher.py']


def generate_english_sources(project):
    """由同一份源码生成英文界面与运行提示，保留中英函数注释和完全相同的模型逻辑。
    Generate English interface and runtime messages from shared code, preserving function documentation and identical model logic."""
    translations = json.loads((project/'translations_en.json').read_text(encoding='utf-8-sig'))
    destination = project/'.packaging/english_source'
    destination.mkdir(parents=True,exist_ok=True)
    for name in MODULES:
        tree = ast.parse((project/name).read_text(encoding='utf-8-sig'))
        docs = {id(node.body[0].value) for node in ast.walk(tree) if isinstance(node,(ast.Module,ast.ClassDef,ast.FunctionDef)) and node.body and isinstance(node.body[0],ast.Expr) and isinstance(node.body[0].value,ast.Constant)}
        for node in ast.walk(tree):
            if isinstance(node,ast.Constant) and isinstance(node.value,str) and id(node) not in docs:
                if any('\u4e00' <= c <= '\u9fff' for c in node.value):
                    if node.value not in translations:
                        raise ValueError('Missing English translation: '+repr(node.value))
                    node.value = translations[node.value]
                elif node.value == 'Microsoft YaHei UI':
                    node.value = 'Segoe UI'
        rendered = ast.unparse(tree)
        if name == 'app.py':
            rendered = rendered.replace('pady=6)', 'pady=4)').replace('pady=16)', 'pady=12)')
        (destination/name).write_text(rendered+'\n',encoding='utf-8')
    return destination
