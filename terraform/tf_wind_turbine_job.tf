# Databricks job resource
resource "databricks_job" "tf_wind_turbines" {
  name                = "tf_wind_turbines"
  max_concurrent_runs = 1

  # Define environment
  environment {
    spec {
      client       = "2"
      dependencies = ["/Volumes/python_wheels/python_wheels/python_wheels/wind-turbines-0.0.1-py3-none-any.whl"]
    }
    environment_key = "wind_turbines_wheel_environment"
  }

  # Enable task queueing
  queue {
    enabled = true
  }

  # Task: ingest - Python Wheel Task
  task {
    task_key = "ingest"

    python_wheel_task {
      package_name = "wind-turbines"
      entry_point  = "ingest_data"
    }

    min_retry_interval_millis = 900000
    disable_auto_optimization = true
    environment_key           = "wind_turbines_wheel_environment"
  }

  # Task: clean - Python Wheel Task
  task {
    task_key = "clean"
    depends_on {
      task_key = "ingest"
    }

    python_wheel_task {
      package_name = "wind-turbines"
      entry_point  = "clean_data"
    }

    min_retry_interval_millis = 900000
    disable_auto_optimization = true
    environment_key           = "wind_turbines_wheel_environment"
  }

  # Task: summary stats - Python Wheel Task
  task {
    task_key = "summary_stats"
    depends_on {
      task_key = "clean"
    }

    python_wheel_task {
      package_name = "wind-turbines"
      entry_point  = "calculate_summary_statistics"
    }

    min_retry_interval_millis = 900000
    disable_auto_optimization = true
    environment_key           = "wind_turbines_wheel_environment"
  }

  # Task: identify anomalies - Python Wheel Task
  task {
    task_key = "anomalies"
    depends_on {
      task_key = "clean"
    }

    python_wheel_task {
      package_name = "wind-turbines"
      entry_point  = "identify_anomalies"
    }

    min_retry_interval_millis = 900000
    disable_auto_optimization = true
    environment_key           = "wind_turbines_wheel_environment"
  }
}

resource "databricks_permissions" "tf_wind_turbines_permissions" {
  job_id = databricks_job.tf_wind_turbines.id

  access_control {
    group_name       = "users"
    permission_level = "CAN_MANAGE_RUN"
  }
}
