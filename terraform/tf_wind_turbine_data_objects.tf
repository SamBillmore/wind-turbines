resource "databricks_schema" "wind_turbines_python_wheel_schema" {
  name         = "wind_turbines_python_wheels"
  catalog_name = data.databricks_catalog.wind_turbines.name
}

resource "databricks_volume" "wind_turbines_python_wheel_volume" {
  name             = "wind_turbines_python_wheels"
  catalog_name     = data.databricks_catalog.wind_turbines.name
  schema_name      = databricks_schema.wind_turbines_python_wheel_schema.name
  volume_type      = "MANAGED"
  depends_on       = [databricks_schema.wind_turbines_python_wheel_schema]
}

resource "databricks_schema" "wind_turbines_raw_schema" {
  name         = "wind_turbines_raw"
  catalog_name = data.databricks_catalog.wind_turbines.name
}

resource "databricks_volume" "wind_turbines_raw_volume" {
  name             = "wind_turbines_raw"
  catalog_name     = data.databricks_catalog.wind_turbines.name
  schema_name      = databricks_schema.wind_turbines_raw_schema.name
  volume_type      = "MANAGED"
  depends_on       = [databricks_schema.wind_turbines_raw_schema]
}

resource "databricks_sql_table" "wind_turbines_raw_table" {
  name               = "wind_turbines_raw"
  catalog_name       = data.databricks_catalog.wind_turbines.name
  schema_name        = databricks_schema.wind_turbines_raw_schema.name
  table_type         = "MANAGED"
  warehouse_id       = data.databricks_sql_warehouse.starter_warehouse.id
  depends_on         = [databricks_schema.wind_turbines_raw_schema]
  column {
    name = "turbine_id"
    type = "INTEGER"
  }
  column {
    name = "timestamp"
    type = "TIMESTAMP"
  }
  column {
    name = "wind_speed"
    type = "DOUBLE"
  }
  column {
    name = "wind_direction"
    type = "INTEGER"
  }
  column {
    name = "power_output"
    type = "DOUBLE"
  }
}

resource "databricks_schema" "wind_turbines_clean_schema" {
  name         = "wind_turbines_clean"
  catalog_name = data.databricks_catalog.wind_turbines.name
}

resource "databricks_sql_table" "wind_turbines_clean_table" {
  name               = "wind_turbines_clean"
  catalog_name       = data.databricks_catalog.wind_turbines.name
  schema_name        = databricks_schema.wind_turbines_clean_schema.name
  table_type         = "MANAGED"
  warehouse_id       = data.databricks_sql_warehouse.starter_warehouse.id
  depends_on         = [databricks_schema.wind_turbines_clean_schema]
  column {
    name = "turbine_id"
    type = "INTEGER"
  }
  column {
    name = "timestamp"
    type = "TIMESTAMP"
  }
  column {
    name = "wind_speed"
    type = "DOUBLE"
  }
  column {
    name = "wind_direction"
    type = "INTEGER"
  }
  column {
    name = "power_output"
    type = "DOUBLE"
  }
}

resource "databricks_schema" "wind_turbines_enriched_schema" {
  name         = "wind_turbines_enriched"
  catalog_name = data.databricks_catalog.wind_turbines.name
}

resource "databricks_sql_table" "wind_turbines_enriched_stats_table" {
  name               = "wind_turbines_summary_statistics"
  catalog_name       = data.databricks_catalog.wind_turbines.name
  schema_name        = databricks_schema.wind_turbines_enriched_schema.name
  table_type         = "MANAGED"
  warehouse_id       = data.databricks_sql_warehouse.starter_warehouse.id
  depends_on         = [databricks_schema.wind_turbines_enriched_schema]
  column {
    name = "turbine_id"
    type = "INTEGER"
  }
  column {
    name = "date"
    type = "DATE"
  }
  column {
    name = "min_power"
    type = "DOUBLE"
  }
  column {
    name = "max_power"
    type = "DOUBLE"
  }
  column {
    name = "avg_power"
    type = "DOUBLE"
  }
}

resource "databricks_sql_table" "wind_turbines_enriched_anomalies_table" {
  name               = "wind_turbines_anomalies_identified"
  catalog_name       = data.databricks_catalog.wind_turbines.name
  schema_name        = databricks_schema.wind_turbines_enriched_schema.name
  table_type         = "MANAGED"
  warehouse_id       = data.databricks_sql_warehouse.starter_warehouse.id
  depends_on         = [databricks_schema.wind_turbines_enriched_schema]
  column {
    name = "turbine_id"
    type = "INTEGER"
  }
  column {
    name = "timestamp"
    type = "TIMESTAMP"
  }
  column {
    name = "wind_speed"
    type = "DOUBLE"
  }
  column {
    name = "wind_direction"
    type = "INTEGER"
  }
  column {
    name = "power_output"
    type = "DOUBLE"
  }
  column {
    name = "mean_power"
    type = "DOUBLE"
  }
  column {
    name = "stddev_power"
    type = "DOUBLE"
  }
  column {
    name = "upper_bound"
    type = "DOUBLE"
  }
  column {
    name = "lower_bound"
    type = "DOUBLE"
  }
  column {
    name = "is_anomaly"
    type = "BOOLEAN"
  }
}
