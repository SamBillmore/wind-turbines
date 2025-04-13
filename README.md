# Wind turbine data processing

Interview case study to process wind turbine data.

## Project overview

This project is to ingest and process data from wind turbines. The turbines generate power based on wind speed and direction, and their output is measured in megawatts (MW).

This project performs the following operations:

1. **Cleans the data**: The raw data contains missing values and outliers, which are either removed or imputed.
2. **Calculates summary statistics**: For each turbine, we calculate the minimum, maximum, and average power output over a given time period (e.g., 24 hours).
3. **Identifies anomalies**: We identify any turbines that have significantly deviated from their expected power output over the same time period. Anomalies are defined as turbines whose output is outside of 2 standard deviations from the mean.

The data is provided as CSVs which are appended daily. Due to the way the turbine measurements are set up, each csv contains data for a group of 5 turbines. Data for a particular turbine will always be in the same file (e.g. turbine 1 will always be in data_group_1.csv).

Each day the csv will be updated with data from the last 24 hours, however the system is known to sometimes miss entries due to sensor malfunctions.

## Solution design overview

### Technologies used

This solution uses PySpark on Databricks for the data processing and data storage.

The code is packaged into semantically versioned python wheel files which are the source for the Databricks jobs.

The objects within Databricks (catalogs, schemas, tables, jobs, etc.) are managed using Terraform.

GitHub actions are used within the development process for CI/CD pipelines.

### Data management

The data is managed into three layers:
1. RAW (a.k.a. Bronze)
    - The raw CSV files in a landing Volume
    - The raw data ingested into delta format, where schemas are enforced
2. CLEAN (a.k.a. Silver)
    - Erroneous and duplicate records are removed and fields are standardised to a consistent format
3. Enriched (a.k.a. Gold)
    - The final output calculations of both the summary statistics and anomaly detection

The solution processes the data incrementally through the use of UPSERT operations within Databricks. This helps reduce compute costs and otimises jobs.

The solution allows for triggering of a new run when a new CSV file is uploaded to the landing location.

### Assumptions made

The files provided are representative of data regularly recorded from the 15 turbines.

Future files will be provided in the same structure and format.

Uploaded data will be accurate and previously processed records will not need to be regularly removed and deleted.

## Setup requirements

The solution uses the Databricks Free Trial with Default Storage.

Databricks:
- Create a Service Principal in the Databricks UI (https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m?language=Terraform#prerequisite-create-a-service-principal)
- Create a catalog called `wind_turbines` with Default Storage (required as we're using the Free Trial of Databricks)

Locally:

- Install dependencies: `pip install -e ".[dev]"`
- If you are contributing to the code, also install the pre-commit hooks: `pre-commit install`
- Install the Databricks CLI
- Install Terraform
- Create a configuration file at `./terraform/env.tfvars` (see the example at `./terraform/env.tfvars.example`)
- Run `databricks auth login --host <host url>`, name the profile `DEFAULT` and authenticate on the Databricks website (NB: this is only required on initial setup)
- Navigate to `./terraform/`
- Run `terraform plan -var-file=env.tfvars -out=tfplan`
- Run `terraform apply "tfplan"`. This will create your schemas, tables, volumes and job.
- Build the python wheel by running (from the root of the repo): `python -m build`

Databricks:

- Upload the csv files to the raw volume `/Volumes/wind_turbines/wind_turbines_raw/wind_turbines_raw`
- Upload the wheel file (in `./dist/`) to python_wheel volume `/Volumes/wind_turbines/wind_turbines_python_wheels/wind_turbines_python_wheels`
- Run the job `tf_wind_turbines`

## Outstanding development required

Here is a list of additional development tasks

- Extend the CI/CD pipelines to include deployment to Databricks
- Extend the CI/CD pipelines to tag the repo with a new semantic version when deploying to Databricks
- Extend the CI/CD pipelines to deploy to multipl environments (e.g. DEV, STG and PRD)
- Replace the Databricks Free Trial with a full implementation and replace the manual creation of a catalog with Terraform creation
- Add integration tests to test the read and write functionality within Databricks (including UPSERT)
