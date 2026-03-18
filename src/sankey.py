import plotly.graph_objects as go
import pandas as pd

# Define the Sankey diagram structure with better flow organization
sankey_data = [
    ['Valid Emails', 'Subscribed', 18_700_000],
    ['Valid Emails', 'Not Subscribed', 14_500_000],

    ['Not Subscribed', 'Transacted in Last 2 years', 6_300_000],
    ['Not Subscribed', 'Not Transacted', 14_500_000 - 6_300_000],

    ['Transacted in Last 2 years', 'Phone Only', 2_700_000],
    ['Transacted in Last 2 years', 'Phone & DM', 1_300_000],
    ['Transacted in Last 2 years', 'DM Only', 671_000],
    ['Transacted in Last 2 years', 'No Contact', 6_300_000 - 2_700_000 - 1_300_000 - 671_000],
    
    ['Subscribed', 'Joann Marketable Universe', 18_700_000],
    ['Phone Only', 'Joann Marketable Universe', 2_700_000],
    ['Phone & DM', 'Joann Marketable Universe', 1_300_000],
    ['DM Only', 'Joann Marketable Universe', 671_000],
    
    ['Joann Marketable Universe', 'Current MIK', 8_100_000],
    ['Joann Marketable Universe', 'Lapsed MIK', 2_000_000],
    ['Joann Marketable Universe', 'Non MIK', 13_000_000]
]

# Create DataFrame
df = pd.DataFrame(sankey_data, columns=['source', 'target', 'value'])

# Get unique node names
all_nodes = list(set(df['source'].tolist() + df['target'].tolist()))

# Create node indices
node_indices = {node: idx for idx, node in enumerate(all_nodes)}

# Define colors for different node types
node_colors = []
for node in all_nodes:
    if 'Total' in node:
        node_colors.append('#1f77b4')  # Blue for total
    elif 'Valid' in node:
        node_colors.append('#2ca02c')  # Green for valid
    elif 'Invalid' in node:
        node_colors.append('#d62728')  # Red for invalid
    elif 'Subscribed' in node:
        node_colors.append('#ff7f0e')  # Orange for subscribed
    elif 'Not Subscribed' in node:
        node_colors.append('#9467bd')  # Purple for not subscribed
    elif 'Transacted' in node:
        node_colors.append('#8c564b')  # Brown for transacted
    elif 'Phone' in node or 'DM' in node:
        node_colors.append('#e377c2')  # Pink for contact methods
    elif 'Marketable' in node:
        node_colors.append('#17becf')  # Cyan for marketable
    elif 'MIK' in node:
        node_colors.append('#bcbd22')  # Yellow-green for MIK
    else:
        node_colors.append('#7f7f7f')  # Gray for others

# Create the Sankey diagram with improved layout
fig = go.Figure(data=[go.Sankey(
    node = dict(
        pad = 30,  # Increased padding between nodes
        thickness = 25,  # Increased node thickness
        line = dict(color = "black", width = 0.5),
        label = [""] * len(all_nodes),  # Empty labels for manual overlay
        color = node_colors,
        x = [0, 0.2, 0.4, 0.6, 0.8, 1],  # Custom x positions for better spacing
        y = [0.5, 0.3, 0.7, 0.2, 0.5, 0.8, 0.1, 0.4, 0.6, 0.9, 0.3, 0.6, 0.9, 0.2, 0.5, 0.8]  # Custom y positions
    ),
    link = dict(
        source = [node_indices[src] for src in df['source']],
        target = [node_indices[tgt] for tgt in df['target']],
        value = df['value'],
        color = ["rgba(0,116,217,0.4)"] * len(df)  # Slightly more transparent
    )
)])

# Update layout with better spacing
fig.update_layout(
    title_text="",  # Remove title for clean overlay
    font_size=11,
    height=800,
    width=1200,
    margin=dict(l=50, r=50, t=50, b=50),
    showlegend=False  # Remove any legends
)

# Save the plot
fig.write_html("joann_improved_sankey.html")

print("=== LABEL-FREE SANKEY DIAGRAM ===")
print("Features:")
print("- All labels removed for manual overlay")
print("- Clean visual flow without text")
print("- Perfect for PowerPoint customization")
print("- Node positions maintained for consistent layout")
print("\nSankey diagram saved as: joann_improved_sankey.html")
print("\nNode positions for reference:")
for i, node in enumerate(all_nodes):
    print(f"  {i}: {node}") 