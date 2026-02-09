#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export DBT_PROFILES_DIR="$PROJECT_ROOT/dbt"
export DBT_PROJECT_DIR="$PROJECT_ROOT/dbt"
export DBT_TARGET="${DBT_TARGET:-local_iceberg}"
export DBT_FILE_FORMAT="${DBT_FILE_FORMAT:-iceberg}"
export DBT_LOCATION_ROOT="${DBT_LOCATION_ROOT:-file://$PROJECT_ROOT/data/iceberg}"
export DBT_ICEBERG_WAREHOUSE="${DBT_ICEBERG_WAREHOUSE:-file://$PROJECT_ROOT/data/iceberg}"

cd "$PROJECT_ROOT/dbt"

dbt --version

dbt deps

dbt build
