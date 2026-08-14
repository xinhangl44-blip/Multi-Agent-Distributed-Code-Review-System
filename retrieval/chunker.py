import tree_sitter_python as tspy
import tree_sitter_go as tsgo
from tree_sitter import Language, Parser

class CodeChunker:
    def __init__(self):
        # 初始化语言和解析器
        self.py_lang = Language(tspy.language())
        self.go_lang = Language(tsgo.language())
        self.parser = Parser()

    def chunk_file(self, file_path: str, content: str) -> list[dict]:
        if file_path.endswith('.py'):
            self.parser.set_language(self.py_lang)
            target_types = ['function_definition', 'class_definition']
        elif file_path.endswith('.go'):
            self.parser.set_language(self.go_lang)
            target_types = ['function_declaration', 'method_declaration', 'type_declaration']
        else:
            return [] # 暂不处理其他类型

        tree = self.parser.parse(bytes(content, "utf8"))
        root_node = tree.root_node
        
        chunks = []
        visited_nodes = set()

        def dfs_extract(node):
            if node.type in target_types and node.id not in visited_nodes:
                # 提取整个节点对应的源码文本
                start_byte = node.start_byte
                end_byte = node.end_byte
                code_snippet = content[start_byte:end_byte]
                
                chunks.append({
                    "file_path": file_path,
                    "type": node.type,
                    "start_line": node.start_point[0] + 1,
                    "end_line": node.end_point[0] + 1,
                    "code": code_snippet
                })
                # 标记该节点已被抽取，防止 class 重复抽取内部的 function
                visited_nodes.add(node.id)
                # 如果是类，还可以继续往内扫它的函数（可选，此处直接返回保留大粒度）
                return 

            for child in node.children:
                dfs_extract(child)

        dfs_extract(root_node)
        
        # 补漏：如果文件没有任何结构化节点（比如只有纯顶层变量或几行赋值），则作为一整个块
        if not chunks and content.strip():
            chunks.append({
                "file_path": file_path,
                "type": "global_block",
                "start_line": 1,
                "end_line": len(content.splitlines()),
                "code": content
            })
            
        return chunks
