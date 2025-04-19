import re
import sys
import os.path
import graphviz
from graphviz import Digraph

class TreeNode:
    """树节点类"""
    def __init__(self, text, level):
        self.text = text          # 节点文本
        self.level = level        # 标题层级
        self.children = []        # 子节点列表
        self.interface = None     # 接口名称
        self.description = None   # 接口描述

def parse_markdown(content):
    """解析Markdown并构建节点树"""
    root = TreeNode("知识图谱", 0)
    current_parent = root
    stack = [root]
    interface_pattern = re.compile(r'接口[:：]\s*(\S+)')
    desc_pattern = re.compile(r'描述[:：]\s*(.+)$')

    lines = content.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # 跳过代码块
        if line.startswith(('```', '~~~')):
            i += 1
            while i < len(lines) and not lines[i].strip().startswith(('```', '~~~')):
                i += 1
            i += 1
            continue
        
        # 标题处理
        if line.startswith('#'):
            level = len(re.match(r'^#+', line).group())
            title = re.sub(r'^#+\s*', '', line).strip()
            
            # 创建新节点
            new_node = TreeNode(title, level)
            
            # 寻找父节点
            while stack[-1].level >= level:
                stack.pop()
            
            # 建立关系
            stack[-1].children.append(new_node)
            stack.append(new_node)
            current_parent = new_node
            i += 1
            
            # 采集接口信息
            while i < len(lines) and not lines[i].startswith('#'):
                clean_line = lines[i].strip()
                if not clean_line:
                    i += 1
                    continue
                
                # 提取接口
                if interface_match := interface_pattern.search(clean_line):
                    new_node.interface = interface_match.group(1)
                
                # 提取描述
                if desc_match := desc_pattern.search(clean_line):
                    new_node.description = desc_match.group(1)
                
                i += 1
        else:
            i += 1
            
    return root

def generate_mindmap(root, filename):
    """生成横向延伸的思维导图"""
    dot = Digraph(
        name='MindMap',
        format='svg',
        engine='dot',
        graph_attr={
            'rankdir': 'LR',       # 改为横向布局
            'bgcolor': '#F7F9FA',
            'splines': 'ortho',    # 直角连线
            'nodesep': '0.8',      # 同级节点间距
            'ranksep': '2.0',      # 层级间距
            'newrank': 'true'      # 启用新布局引擎
        },
        node_attr={
            'style': 'filled',
            'fontname': 'Microsoft YaHei',
            'fontsize': '12',
            'shape': 'rect',
            'penwidth': '0',
            'margin': '0.2,0.1'
        }
    )
    
    # 颜色方案
    color_map = {
        'root': ['#2C3E50', 'white'],
        'default': ['#2980B9', 'white'],
        'interface': ['#27AE60', 'white']
    }
    
    # 递归添加节点
    def add_nodes(node, parent_id=None):
        node_id = str(id(node))
        
        # 节点标签
        label = node.text
        if node.interface:
            label += '\n接口: ' + node.interface
        if node.description:
            label += '\n描述: ' + node.description
        
        # 节点样式
        fill_color = color_map['default'][0]
        font_color = color_map['default'][1]
        if node.level == 0:
            fill_color = color_map['root'][0]
            font_color = color_map['root'][1]
        elif node.interface:
            fill_color = color_map['interface'][0]
            font_color = color_map['interface'][1]
        
        dot.node(
            node_id,
            label=label,
            fillcolor=fill_color,
            fontcolor=font_color,
            fontsize='14' if node.level <= 2 else '12',
            gradientangle='270',
            width='1.5' if node.level <= 2 else '1.2',
            height='0.6'
        )
        
        # 创建连接
        if parent_id:
            dot.edge(
                parent_id, node_id,
                color='#95A5A6',
                penwidth='1.5',
                arrowsize='0.7'
            )
            
        # 纵向排列子节点
        with dot.subgraph() as s:
            s.attr(rank='same')
            for child in node.children:
                add_nodes(child, node_id)
    
    add_nodes(root)
    
    # 生成文件
    dot.render(filename, cleanup=True)
    print(f"已生成横向布局文件：{filename}.svg")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("使用方式：python M2S.py input.md output")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_name = sys.argv[2]
    
    # 获取输入文件所在目录
    output_dir = os.path.dirname(os.path.abspath(input_file))
    # 构建输出文件完整路径
    output_path = os.path.join(output_dir, output_name)
    
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    root = parse_markdown(content)
    generate_mindmap(root, output_path)