import tree_sitter_python as tspy
import tree_sitter_go as tsgo
from tree_sitter import Language, Parser

class CodeChunker:
    def __init__(self):
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
            return []
        tree = self.parser.parse(bytes(content, "utf8"))
        root_node = tree.root_node
        chunks = []
        visited_nodes = set()

        def dfs_extract(node):
            if node.type in target_types and node.id not in visited_nodes:
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
                visited_nodes.add(node.id)
                return 
            for child in node.children:
                dfs_extract(child)
        dfs_extract(root_node)
        if not chunks and content.strip():
            chunks.append({
                "file_path": file_path,
                "type": "global_block",
                "start_line": 1,
                "end_line": len(content.splitlines()),
                "code": content
            })
        return chunks
