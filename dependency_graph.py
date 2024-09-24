from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import re
from collections import defaultdict, deque
import networkx as nx
import matplotlib.pyplot as plt

# Set up your project and datasets
OLD_PROJECT_ID = "sentiment-forecast"
NEW_PROJECT_ID = "ai-playground-prj"
OLD_DATASET = "binance"
NEW_DATASET = "binance"

# Initialize BigQuery client
client = bigquery.Client()

# Step 1: Gather view queries and dependencies
view_dependencies = defaultdict(list)
view_queries = {}
view_names = []

# Fetch views from the old dataset
views = client.list_tables(OLD_DATASET)
for table in views:
    if table.table_type == 'VIEW':
        view_names.append(table.table_id)
        
        # Get the original view query
        view_ref = f"{OLD_PROJECT_ID}.{OLD_DATASET}.{table.table_id}"
        view = client.get_table(view_ref)
        view_query = view.view_query
        
        # Clean the query for the new dataset
        view_query_cleaned = re.sub(rf"{OLD_PROJECT_ID}\.{OLD_DATASET}\.", f"{NEW_PROJECT_ID}.{NEW_DATASET}.", view_query)
        view_query_cleaned = re.sub(rf"{OLD_DATASET}\.", f"{NEW_DATASET}.", view_query_cleaned)
        
        # Store the cleaned query for later use
        view_queries[table.table_id] = view_query_cleaned
        
        # Find dependencies
        dependencies = re.findall(rf"{OLD_PROJECT_ID}\.{OLD_DATASET}\.([a-zA-Z0-9_]+)|{OLD_DATASET}\.([a-zA-Z0-9_]+)", view_query)
        dependencies_flat = [dep[0] or dep[1] for dep in dependencies]
        view_dependencies[table.table_id].extend(dependencies_flat)


# Step 2: Generate the dependency graph
G = nx.DiGraph()

# Add edges based on view dependencies
for view, dependencies in view_dependencies.items():
    for dependency in dependencies:
        G.add_edge(dependency, view)

# Draw the graph with adjustments
plt.figure(figsize=(15, 10))  # Increase figure size for better clarity

# Try a different layout to minimize overlap
pos = nx.spring_layout(G, k=0.5, iterations=50)  # Adjust k for spacing
nx.draw(G, pos, with_labels=True, node_size=3000, node_color="lightblue", 
        font_size=10, font_weight="bold", arrows=True, connectionstyle='arc3,rad=0.1')

# Improve visibility
plt.title("View Dependency Graph", fontsize=16)
plt.axis('off')  # Hide axes for better presentation

# Save the graph as an image
plt.savefig("view_dependency_graph.png", format="png", bbox_inches='tight')  # bbox_inches='tight' to reduce clipping
plt.close()

# Step 3: Topological Sorting
in_degree = {view: 0 for view in view_names}

# Calculate in-degrees of each view
for dependencies in view_dependencies.values():
    for dependency in dependencies:
        if dependency not in in_degree:
            in_degree[dependency] = 0  
        in_degree[dependency] += 1

# Collect all views with 0 in-degree
queue = deque([view for view in view_names if in_degree[view] == 0])
sorted_views = []

# Process views in the correct order
while queue:
    current_view = queue.popleft()
    sorted_views.append(current_view)

    # Reduce in-degree for each dependent view
    for dependency in view_dependencies[current_view]:
        in_degree[dependency] -= 1
        if in_degree[dependency] == 0:
            queue.append(dependency)

# Step 4: Create views in the correct order
for view in sorted_views:
    if view not in view_queries:
        continue  # Skip if no query is found

    view_query_cleaned = view_queries[view]
    new_view_ref = f"{NEW_PROJECT_ID}.{NEW_DATASET}.{view}"

    try:
        client.get_table(new_view_ref)  # Check if the view exists
        print(f"View {view} already exists. Skipping creation.")
        continue
    except NotFound:
        pass  # If the view does not exist, we can create it

    # Try creating the view
    new_view = bigquery.Table(new_view_ref)
    new_view.view_query = view_query_cleaned
    client.create_table(new_view)  # This creates the view
    print(f"Successfully created view {view}")

# Step 5: Generate DDL Scripts
with open("create_views_ddl.sql", "w") as ddl_file:
    for view in sorted_views:
        ddl_statement = f"CREATE OR REPLACE VIEW `{new_view_ref}` AS {view_query_cleaned};\n"
        ddl_file.write(ddl_statement)

print("Dependency graph has been saved as view_dependency_graph.png.")
