import duckdb
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
from pathlib import Path
import numpy as np

def create_interactive_graph(db_path: str, output_html: str = "graph_interactive.html"):
    # Connect & sample data
    conn = duckdb.connect(db_path)
    
    # Get focused sample (TEST-CRU-CHUNK paths)
    nodes = conn.execute("""
        WITH test_sample AS (
            SELECT node_id FROM nodes WHERE node_type='TEST' LIMIT 30
        )
        SELECT node_id, node_type, title 
        FROM nodes 
        WHERE node_type IN ('TEST', 'CRU', 'CHUNK')
           OR node_id IN (
               SELECT dst_id FROM edges WHERE src_id IN (SELECT node_id FROM test_sample)
               UNION ALL
               SELECT dst_id FROM edges WHERE src_id IN (
                   SELECT dst_id FROM edges WHERE src_id IN (SELECT node_id FROM test_sample)
               )
           )
        LIMIT 80
    """).fetchdf()
    
    edges = conn.execute("""
        SELECT src_id, rel_type, dst_id, confidence
        FROM edges 
        WHERE src_id IN (SELECT node_id FROM nodes LIMIT 80)
           OR dst_id IN (SELECT node_id FROM nodes LIMIT 80)
        LIMIT 120
    """).fetchdf()
    
    conn.close()
    
    print(f"Graph: {len(nodes)} nodes, {len(edges)} edges")
    
    # NetworkX for layout
    G = nx.from_pandas_edgelist(edges, 'src_id', 'dst_id', edge_attr='rel_type')
    nx.set_node_attributes(G, dict(zip(nodes.node_id, nodes.node_type)), 'type')
    
    # Spring layout
    pos = nx.spring_layout(G, k=1.5, iterations=100, seed=42)
    
    # Edge traces (colored by type)
    edge_traces = []
    edge_types = edges['rel_type'].unique()
    colors = {'TESTS':'#e74c3c', 'SUPPORTED_BY':'#3498db', 'PARENT_OF':'#2ecc71'}
    
    for etype in edge_types:
        edge_subset = edges[edges['rel_type'] == etype]
        edge_x, edge_y = [], []
        
        for _, edge in edge_subset.iterrows():
            x0, y0 = pos[edge['src_id']]
            x1, y1 = pos[edge['dst_id']]
            edge_x += [x0, x1, None]
            edge_y += [y0, y1, None]
        
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=4, color=colors.get(etype, '#888')),
            hoverinfo='none',
            mode='lines',
            name=etype,
            legendgroup=etype,
            showlegend=True
        )
        edge_traces.append(edge_trace)
    
        # Node trace
    node_x, node_y, node_text = [], [], []
    node_colors = []

    node_type_colors = {
        'TEST': '#e74c3c',
        'CRU': '#3498db',
        'CHUNK': '#2ecc71'
    }

    for node_id, attrs in G.nodes(data=True):
        x, y = pos[node_id]
        node_x.append(x)
        node_y.append(y)

        node_type = attrs.get('type', '?')
        node_text.append(f"<b>{node_id[:20]}</b><br>Type: {node_type}<br>Module: SRS")
        node_colors.append(node_type_colors.get(node_type, '#95a5a6'))
    
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        text=node_text,
        textposition="middle center",
        hoverinfo='text',
        marker=dict(size=18, color=node_colors, line=dict(width=2, color='white')),
        name='Nodes',
        showlegend=False
    )
    
    # Layout
    fig = go.Figure(data=edge_traces + [node_trace])
    fig.update_layout(
        title=dict(text="Interactive GraphRAG Knowledge Graph<br><sub>TEST → CRU → CHUNK | Click+drag to pan, scroll to zoom</sub>", 
                  x=0.5, font=dict(size=20)),
        width=1400, height=1000,
        showlegend=True,
        hovermode='closest',
        margin=dict(t=100, b=20, l=20, r=20),
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    fig.write_html(output_html, config={'scrollZoom': True, 'displayModeBar': True})
    print(f"✅ Interactive graph: {output_html}")
    print(f"Open in browser: file://{Path(output_html).resolve()}")
    
    return fig

if __name__ == "__main__":
    # From output/ folder, use ../test_graph.duckdb
    create_interactive_graph("test_graph.duckdb")