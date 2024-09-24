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

