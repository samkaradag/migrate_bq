# BigQuery View Migration Tool

This Python script automates the migration of dependent views from one Google Cloud BigQuery project to another. It handles views that reference other views, ensuring they are created in the correct order based on their dependencies.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Requirements](#requirements)
- [Setup](#setup)
- [Usage](#usage)
- [Key Components](#key-components)
- [License](#license)

## Introduction

In a recent migration project, we faced the challenge of migrating a dataset containing numerous interdependent views. This tool simplifies the migration process by automating the extraction, transformation, and creation of views in a new BigQuery project.

## Features

- Automatically resolves view dependencies and creates views in the correct order.
- Handles existing views with options to replace or skip creation.
- Updates view definitions to reflect the new project and dataset names.
- Outputs a DDL (Data Definition Language) script for the created views.

## Requirements

- Python 3.6 or higher
- Google Cloud SDK
- `google-cloud-bigquery` library

To install the required library, run:

```bash
pip install google-cloud-bigquery
```

## Setup

1. Clone the repository: `git clone <repository_url>` (replace `<repository_url>` with your actual repository URL).
2. Navigate to the project directory: `cd bigquery-view-migration`
3. Set up authentication for Google Cloud SDK by running: `gcloud auth login`
4. Update the script (`copy_views.py`) with your project and dataset details:
- `OLD_PROJECT_ID`: The project ID of the source dataset.
- `NEW_PROJECT_ID`: The project ID of the destination dataset.
- `OLD_DATASET`: The dataset name in the source project.
- `NEW_DATASET`: The dataset name in the destination project.

## Usage

Open the script `copy_views.py` and set the following flags:

- `REPLACE_VIEWS`: Set to `True` to replace existing views in the target dataset; `False` to skip them.
- `EXECUTE_CREATION`: Set to `True` to execute the view creation in BigQuery; `False` to only generate the DDL script.

Run the script: `python copy_views.py`

After execution, check the output file `create_views_ddl.sql` for the DDL statements of the created views.


## Key Components

- **Dependency Resolution:** Uses Kahn's algorithm for topological sorting to determine the correct order for view creation.
- **Query Transformation:** Cleans up view queries to reflect the new dataset and project.
- **Error Handling:** Gracefully handles existing views and missing dependencies.

