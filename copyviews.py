from google.cloud import bigquery
import re
from google.api_core.exceptions import NotFound
from collections import defaultdict, deque

# Set up your project and datasets
OLD_PROJECT_ID = "your_old_project_id"
NEW_PROJECT_ID = "your_new_project_id"
OLD_DATASET = "your_old_dataset"
NEW_DATASET = "your_new_dataset"

# Flag to determine whether to skip or replace existing views
REPLACE_VIEWS = False  # Set to True to replace existing views; False to skip
EXECUTE_CREATION = True  # Set to False to skip actual creation of views in BigQuery

# Path to output the DDL script
DDL_OUTPUT_FILE = "create_views_ddl.sql"

client = bigquery.Client(project=OLD_PROJECT_ID)

# Step 1: Gather view queries and detect dependencies
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
        try:
            view = client.get_table(view_ref)
            view_query = view.view_query

            # Clean the query for the new dataset
            # Replace both dataset references and project references
            view_query_cleaned = re.sub(rf"{OLD_PROJECT_ID}\.{OLD_DATASET}\.", f"{NEW_PROJECT_ID}.{NEW_DATASET}.", view_query)
            view_query_cleaned = re.sub(rf"{OLD_DATASET}\.", f"{NEW_DATASET}.", view_query_cleaned)

            # Store the cleaned query for later use
            view_queries[table.table_id] = view_query_cleaned

            # Find dependencies (other views referenced in this view's query)
            dependencies = re.findall(rf"{OLD_PROJECT_ID}\.{OLD_DATASET}\.([a-zA-Z0-9_]+)|{OLD_DATASET}\.([a-zA-Z0-9_]+)", view_query)
            dependencies_flat = [dep[0] or dep[1] for dep in dependencies]  # Flatten tuple
            view_dependencies[table.table_id].extend(dependencies_flat)

        except NotFound:
            print(f"View {view_ref} not found.")
            continue

# Debugging: Print the detected views and their dependencies
print("Detected Views and Dependencies:")
for view in view_dependencies:
    print(f"View: {view}, Dependencies: {view_dependencies[view]}")

# Step 2: Perform topological sorting using Kahn's Algorithm (iterative)
in_degree = {view: 0 for view in view_names}

# Calculate in-degrees of each view
for dependencies in view_dependencies.values():
    for dependency in dependencies:
        if dependency not in in_degree:
            in_degree[dependency] = 0  
        in_degree[dependency] += 1

# Collect all views with 0 in-degree (no dependencies)
queue = deque([view for view in view_names if in_degree[view] == 0])
sorted_views = []

# Process views iteratively in the correct order
while queue:
    current_view = queue.popleft()
    sorted_views.append(current_view)

    # Reduce in-degree for each dependent view
    for dependency in view_dependencies[current_view]:
        in_degree[dependency] -= 1
        if in_degree[dependency] == 0:
            queue.append(dependency)

# Check for circular dependencies
circular_views = [view for view, degree in in_degree.items() if degree > 0]
if circular_views:
    print("Warning: Circular dependencies detected in the following views:")
    for view in circular_views:
        print(f"  - {view}")
    print("Attempting to create the remaining views despite circular dependencies.")

# Debugging: Print the sorted views
print("Sorted Views for Creation:")
for view in sorted_views:
    print(view)

# Step 3: Create views in topologically sorted order and output DDL
with open(DDL_OUTPUT_FILE, "w") as ddl_file:
    for view in sorted_views:
        # Check if the view query exists
        if view not in view_queries:
            print(f"Warning: No query found for view {view}. Skipping.")
            continue  # Skip if no query is found

        view_query_cleaned = view_queries[view]
        print(f"Creating view: {view} with query:")
        print(view_query_cleaned)
        print("----------------------------------------")

        new_view_ref = f"{NEW_PROJECT_ID}.{NEW_DATASET}.{view}"

        if REPLACE_VIEWS:
            # Try creating or replacing the view
            if EXECUTE_CREATION:
                try:
                    new_view = bigquery.Table(new_view_ref)
                    new_view.view_query = view_query_cleaned
                    new_view = client.create_table(new_view, exists_ok=True)  # This creates or replaces the view
                    print(f"Successfully created or replaced view {view}")
                except Exception as e:
                    print(f"Failed to create or replace view {view}. Error: {e}")
            else:
                print(f"DDL for view {view} would be executed (not actually creating).")
        else:
            # Check if the view already exists before creating it
            try:
                client.get_table(new_view_ref)  # Check if the view exists
                print(f"View {view} already exists. Skipping creation.")
                continue  # Skip this view if it already exists
            except NotFound:
                # If the view does not exist, create it
                pass

            # Try creating the view
            if EXECUTE_CREATION:
                try:
                    new_view = bigquery.Table(new_view_ref)
                    new_view.view_query = view_query_cleaned
                    new_view = client.create_table(new_view)  # This creates the view
                    print(f"Successfully created view {view}")
                except Exception as e:
                    print(f"Failed to create view {view}. Error: {e}")
            else:
                print(f"DDL for view {view} would be executed (not actually creating).")

        # Write the DDL to the file
        ddl_statement = f"CREATE OR REPLACE VIEW `{new_view_ref}` AS {view_query_cleaned};\n"
        ddl_file.write(ddl_statement)

print(f"View copying process completed. DDL script saved to {DDL_OUTPUT_FILE}.")
