# Reproducing the demonstration

## View the existing outputs

Open `powerbi/LEGO_Retail.pbix` in Power BI Desktop. The supplied report contains synthetic snapshot data. The PDF and CSV exports can be reviewed without Databricks or Power BI.

## Run the pipeline

1. Use a Databricks workspace with Python, Spark, Delta Lake, and permission to create a demonstration schema in the current catalog.
2. Import `notebooks/LEGO_Retail_Demand_Supply_Intelligence.ipynb` as a notebook. The `.py` file is an alternative Databricks source export.
3. Read the configuration and overwrite notice before running. The code uses the current catalog and dedicated `lego_portfolio_demo` schema. Use a separate demo catalog if that name already contains valuable work.
4. Run all cells in order. The simulation is deterministic; runtime identifiers and timings vary.
5. Inspect quality-gate results and Gold table outputs. The published evidence is in `results/`.
6. The optional final export writes a ZIP to the managed export volume. If volume creation is unavailable, inspect the Delta tables directly. See notebook instructions for the Catalog download path.

## Refresh the report with another run

The included Power BI project embeds a fixed data snapshot in Power Query. Re-running Databricks does not refresh that embedded snapshot automatically. To use new CSV outputs, replace the six fact/dimension queries in Power Query with the corresponding CSV sources, preserve column names and types, review all relationships and measures, and reconcile cards with that run's `runtime_results.json` before saving a new PBIX.

The supplied notebook is source, rather than a notebook with retained interactive execution outputs. Runtime JSON and CSV checks are the published execution evidence.
