# Data dictionary and metric definitions

All records are synthetic. CSV date columns use ISO dates; quantities are whole units except forecast estimates, and monetary values are synthetic USD.

## Tables and columns

| File | Rows | Columns |
|---|---:|---|
| `gold_dim_store.csv` | 12 | `store_id`, `store_name`, `region` |
| `gold_dim_product.csv` | 24 | `product_id`, `sku`, `product_name`, `theme`, `list_price` |
| `gold_dim_date.csv` | 180 | `date`, `year`, `month`, `year_month`, `weekday` |
| `gold_fact_daily.csv` | 51,840 | `date`, `store_id`, `product_id`, `sales_units`, `demand_units`, `sales_amount`, `unit_price`, `promo_flag`, `opening_units`, `received_units`, `closing_units`, `lost_units`, `stockout_exposure`, `available_flag`, `stockout_flag` |
| `gold_fact_forecast.csv` | 8,064 | `store_id`, `product_id`, `weekday`, `date`, `demand_units`, `forecast_units`, `forecast_origin`, `absolute_error`, `signed_error`, `model_name` |
| `gold_fact_replenishment.csv` | 6,564 | `order_id`, `store_id`, `product_id`, `requested_units`, `shipped_units`, `lead_days`, `order_date`, `expected_date` |
| `gold_sales_performance.csv` | 2,160 | `date`, `store_id`, `revenue`, `units_sold`, `demand_units` |
| `gold_inventory_availability.csv` | 2,160 | `date`, `store_id`, `product_days`, `available_product_days`, `stockout_product_days`, `closing_units`, `lost_units`, `stockout_exposure` |
| `gold_forecast_performance.csv` | 288 | `store_id`, `product_id`, `actual_units`, `forecast_units`, `absolute_error`, `signed_error`, `wape`, `bias` |
| `gold_supplier_fill.csv` | 2,049 | `order_date`, `store_id`, `requested_units`, `shipped_units`, `supplier_fill_rate` |
| `quality_checks.csv` | 18 | `run_id`, `check_name`, `actual`, `expected`, `passed` |

## Grains and relationships

- `gold_dim_store`: one row per store; key `store_id`.
- `gold_dim_product`: one row per product; key `product_id`.
- `gold_dim_date`: one row per date; key `date`.
- `gold_fact_daily`: date × store × product. Inventory balances, receipts, demand, fulfilled sales, and modeled exposure.
- `gold_fact_forecast`: holdout target date × store × product for a fixed forecast origin. Retain origin/model fields when comparing future runs.
- `gold_fact_replenishment`: one synthetic order per `order_id`. Requested and shipped quantities; order and expected arrival dates. Expected date is not proof of on-time receipt.
- Reporting marts aggregate the facts for sales, inventory, forecast, and supplier performance; inspect the column list for their grouping fields.

All facts join to Store and Product. Daily and forecast use target date; replenishment uses order date. Dimensions filter facts in one direction. Do not join fact tables directly or sum inventory across dates.

## Metric rules

Consumer fill = SUM(sales_units) / SUM(demand_units). Supplier fill = SUM(shipped_units) / SUM(requested_units). These describe different processes.

Daily availability = average available_flag; available means opening + received units > 0. Stockout day rate = average stockout_flag; a row can have some stock and still fail to meet all demand, so the two rates are not complements.

Lost demand = demand_units − sales_units. Modeled exposure = lost units × daily price. It is neither audited lost revenue nor a demonstrated saving.

WAPE = SUM(absolute_error) / SUM(demand_units) at the forecast row grain. Bias = SUM(signed_error) / SUM(demand_units), where signed error = forecast − demand. Avoid averaging row-level percentages.

Closing inventory is semiadditive: sum stores/products at the latest selected date, not all daily closing balances.
