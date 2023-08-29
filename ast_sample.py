import ast
import os
def extract_names_from_ast(node):
    """
    PythonのASTノードから関数とクラスの名前を抽出します。
    """
    if isinstance(node, ast.FunctionDef):
        
        print(f"Function: {node.name}: {node.lineno} - {node.end_lineno}" )
    elif isinstance(node, ast.ClassDef):
        print(f"Class: {node.name}:  {node.lineno} - {node.end_lineno}")
    # 再帰的に子ノードを探索
    for child in ast.iter_child_nodes(node):
        extract_names_from_ast(child)
# サンプルのソースコード
source_code = """
import os

text_path = os.path.join(os.path.curdir,"test.txt")
def function_a():
    pass

def function_b(x, y):
    return x + y

class MyClass:
    def method_1(self):
        pass
    def method_2(self):
        pass
"""

# ソースコードをASTに変換
parsed_ast = ast.parse(source_code)

# ASTから関数名とクラス名を抽出
extract_names_from_ast(parsed_ast)
