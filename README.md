TODO:
- Run on Databricks
- Tidy up README
- Mention integration tests
- Submit



# Wind turbine data processing

Interview case study to process wind turbine data.

## Project overview

This project is to ingest and process data from wind turbines. The turbines generate power based on wind speed and direction, and their output is measured in megawatts (MW).

This project performs the following operations:

1. **Cleans the data**: The raw data contains missing values and outliers, which are either removed or imputed.
2. **Calculates summary statistics**: For each turbine, we calculate the minimum, maximum, and average power output over a given time period (e.g., 24 hours).
3. **Identifies anomalies**: We identify any turbines that have significantly deviated from their expected power output over the same time period. Anomalies are defined as turbines whose output is outside of 2 standard deviations from the mean.
4. **Stores the processed data**: Store the cleaned data and summary statistics in a database for further analysis.

The data is provided as CSVs which are appended daily. Due to the way the turbine measurements are set up, each csv contains data for a group of 5 turbines. Data for a particular turbine will always be in the same file (e.g. turbine 1 will always be in data_group_1.csv).

Each day the csv will be updated with data from the last 24 hours, however the system is known to sometimes miss entries due to sensor malfunctions.

## Solution design overview

- UPSERT

## Assumptions made

The files provided in the attachment represent a valid set for a month of data recorded from the
15 turbines. Feel free to add/remove data from the set provided in order to test/satisfy the
requirements above.

## Setup requirements

- Uses Databricks Free Trial with Default Storage
- Install Databricks CLI
- Install Terraform
- Create a Service Principal in UI (https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m?language=Terraform#prerequisite-create-a-service-principal)
- Manually create catalog called `wind_turbines` with Default Storage
- Create configuration (`deployment/terraform/env.tfvars`)
- Run `databricks auth login --host <host url>` and authenticate on website (only required on initial setup)
- Navigate to `terraform/`
- Run `terraform plan -var-file=env.tfvars -out=tfplan`
- Run `terraform apply "tfplan" `
- Upload csv files to raw volume
- Build python wheel: `python -m build`
- Upload wheel file to python_wheel volume

## Additional development setup

- Dependencies: `pip install -e ".[dev]"`
- Pre-commit hooks: `pre-commit install`
