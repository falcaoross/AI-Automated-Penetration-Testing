import duckdb
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path
import json

def visualize_graph(db_path: str, output_png: str = "graph_viz.png"):
    conn = duckdb.connect(db_path)
    
    # Get stats
    stats = {
        'nodes': conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0],
        'edges': conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
    }
    print(f"Graph: {stats['nodes']} nodes, {stats['edges']} edges")
    
    # Sample for visualization (prevents memory issues)
    nodes = conn.execute("""
        SELECT node_id, node_type, title 
        FROM nodes 
        ORDER BY RANDOM() 
        LIMIT 50
    """).fetchdf()
    
    edges = conn.execute("""
        SELECT src_id, rel_type, dst_id 
        FROM edges 
        WHERE src_id IN (SELECT node_id FROM nodes LIMIT 50)
           OR dst_id IN (SELECT node_id FROM nodes LIMIT 50)
        LIMIT 100
    """).fetchdf()
    
    conn.close()
    
    print(f"Viz sample: {len(nodes)} nodes, {len(edges)} edges")
    
    # Build graph
    G = nx.from_pandas_edgelist(edges, 'src_id', 'dst_id', edge_attr='rel_type')
    nx.set_node_attributes(G, dict(zip(nodes.node_id, nodes.node_type)), 'type')
    
    # Visualize
    plt.figure(figsize=(16, 12))
    pos = nx.spring_layout(G, k=1.5, iterations=100)
    
    colors = {'TEST':'#e74c3c', 'CRU':'#3498db', 'CHUNK':'#2ecc71'}
    node_colors = [colors.get(G.nodes[n].get('type', 'gray'), 'gray') for n in G.nodes]
    
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=1500, alpha=0.85,
                          edgecolors='white', linewidths=2)
    nx.draw_networkx_edges(G, pos, alpha=0.5, width=2, arrows=True)
    nx.draw_networkx_labels(G, pos, font_size=8)
    
    plt.title(f'GraphRAG: {stats["nodes"]} nodes | Sample visualization', fontsize=16)
    plt.axis('off')
    plt.tight_layout()
    plt.savefig(output_png, dpi=300, bbox_inches='tight')
    plt.show()
    print(f"✅ Saved: {output_png}")

if __name__ == "__main__":
    visualize_graph("output/test_graph.duckdb")