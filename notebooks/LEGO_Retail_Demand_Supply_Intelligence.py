# Databricks notebook source
# MAGIC %md
# MAGIC # LEGO Retail Demand & Supply Intelligence
# MAGIC **Tony Poulakos | Independent technical portfolio demonstration**
# MAGIC 
# MAGIC Synthetic data only. Not affiliated with, commissioned by, or deployed within the LEGO Group. Store locations, product names, prices, demand, inventory, and operational outcomes are invented. This is a small batch prototype, not an enterprise production deployment.
# MAGIC 
# MAGIC **Run status:** This delivered notebook has not been executed in Databricks. Run all cells on Free Edition serverless compute; only successful runtime outputs constitute results. No external data, credentials, libraries, or paid services are required.
# MAGIC 
# MAGIC Architecture: Python simulator → Bronze raw Delta → PySpark validation/quarantine → Silver conformed Delta → SQL Gold star schema and KPI marts → Power BI CSV extracts. Tables are confined to the dedicated `lego_portfolio_demo` schema. Re-running replaces only this demonstration's named tables and exports.
# MAGIC 
# MAGIC Scope: 12 fictional stores × 24 fictional products × 180 days (51,840 store-product-days), plus purchase orders and a forecast evaluated on the last 28 days. All monetary values are synthetic USD; no currency conversion is implied.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
import random, math, datetime as dt, json, time, uuid
RUN_ID = str(uuid.uuid4())
STARTED = time.time()
SEED = 20261031
SCHEMA = "lego_portfolio_demo"
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
assert CATALOG and "`" not in CATALOG
NS = f"`{CATALOG}`.`{SCHEMA}`"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {NS}")
def table(name):
    assert name.replace("_", "").isalnum()
    return f"{NS}.`{name}`"
def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table(name))
def sql_save(name, query):
    spark.sql(f"CREATE OR REPLACE TABLE {table(name)} USING DELTA AS {query}")
print(f"Run {RUN_ID}; dedicated namespace {CATALOG}.{SCHEMA}; seed {SEED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reproducible synthetic source systems
# MAGIC The simulator models daily unconstrained demand, weekend traffic, seasonality, a promotion, stock-constrained sales, inventory movement, replenishment lead times, and partially fulfilled orders. Replenishment uses **only previous demand observations**, never future demand. Latent demand is available because this is a simulation; a real retailer would need a separate lost-sales estimation method.
# MAGIC 
# MAGIC Products use fictional demo identifiers rather than actual LEGO set numbers. Stores are fictional nodes in illustrative US markets. Bronze sales include three deliberately bad records and five duplicate deliveries to demonstrate quarantine and deduplication.

# COMMAND ----------

rng = random.Random(SEED)
start = dt.date(2026, 1, 1)
DAYS = 180
stores = [(i+1, f"Demo {city}", region, float(0.80+i*0.045)) for i,(city,region) in enumerate([
("Boston","Northeast"),("New York","Northeast"),("Philadelphia","Northeast"),
("Chicago","Central"),("Minneapolis","Central"),("Dallas","Central"),
("Atlanta","South"),("Miami","South"),("Orlando","South"),
("Seattle","West"),("Portland","West"),("San Diego","West")])]
themes = ["City-inspired", "Space-inspired", "Creative bricks", "Vehicles-inspired"]
products = [(p, f"DEMO-{p:03d}", f"Fictional building set {p:02d}", themes[(p-1)//6],
             float([19.99,29.99,49.99,79.99,99.99,149.99][(p-1)%6]), float(1.2+(p%6)*0.38)) for p in range(1,25)]
sales, inventories, orders = [], [], []
for sid,_,_,store_factor in stores:
    for pid,_,_,_,price,base in products:
        stock = 20
        pending = []
        history = []
        for day in range(DAYS):
            date = start + dt.timedelta(days=day)
            opening = stock
            receipts = sum(q for arrival,q in pending if arrival == day)
            pending = [(arrival,q) for arrival,q in pending if arrival > day]
            available = opening + receipts
            promo = 60 <= day < 74 and pid % 4 == 0
            mean = base*store_factor*(1.45 if date.weekday() >= 5 else 1.0)*(1+0.15*math.sin(day/28))*(1.5 if promo else 1)
            # Knuth's Poisson algorithm, bounded means for this small synthetic demo.
            L, k, prob = math.exp(-mean), 0, 1.0
            while prob > L:
                k += 1
                prob *= rng.random()
            demand = k-1
            units = min(demand, available)
            stock = available-units
            net_price = round(price*(0.85 if promo else 1), 2)
            key = f"{date.isoformat()}-{sid}-{pid}"
            sales.append((key,date.isoformat(),sid,pid,demand,units,net_price,round(units*net_price,2),int(promo)))
            inventories.append((date.isoformat(),sid,pid,opening,receipts,stock))
            history.append(demand)
            prior_mean = sum(history[-28:])/len(history[-28:])
            inventory_position = stock + sum(q for _,q in pending)
            if inventory_position < prior_mean*5:
                requested = max(1, math.ceil(prior_mean*12-inventory_position))
                shipped = max(0, math.floor(requested*rng.choice([0.65,0.85,1.0,1.0,1.0])))
                lead = rng.choice([3,4,5,7])
                oid = f"PO-{sid}-{pid}-{day}"
                orders.append((oid,date.isoformat(),(date+dt.timedelta(days=lead)).isoformat(),sid,pid,requested,shipped,lead))
                pending.append((day+lead,shipped))
raw_sales = sales + sales[:5] + [
("BAD-NEG","2026-01-01",1,1,-1,-1,19.99,-19.99,0),
("BAD-STORE","2026-01-01",999,1,1,1,19.99,19.99,0),
("BAD-DATE","invalid-date",1,1,1,1,19.99,19.99,0)]
print({"clean_sales":len(sales),"raw_sales":len(raw_sales),"inventory":len(inventories),"replenishment_orders":len(orders)})

# COMMAND ----------

sources = {
"bronze_stores": spark.createDataFrame(stores,"store_id int, store_name string, region string, store_factor double"),
"bronze_products": spark.createDataFrame(products,"product_id int, sku string, product_name string, theme string, list_price double, base_demand double"),
"bronze_sales": spark.createDataFrame(raw_sales,"record_id string, date_raw string, store_id int, product_id int, demand_units int, sales_units int, unit_price double, sales_amount double, promo_flag int"),
"bronze_inventory": spark.createDataFrame(inventories,"date_raw string, store_id int, product_id int, opening_units int, received_units int, closing_units int"),
"bronze_replenishment": spark.createDataFrame(orders,"order_id string, order_date_raw string, expected_date_raw string, store_id int, product_id int, requested_units int, shipped_units int, lead_days int")}
for name, df in sources.items():
    save(df.withColumn("ingested_at",F.current_timestamp()).withColumn("run_id",F.lit(RUN_ID)),name)
print("Bronze persisted as managed Delta tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver validation, quarantine, and conformance
# MAGIC Checks cover dates, reference keys, nonnegative quantities, sales ≤ demand, and arithmetic consistency. Duplicate source deliveries are removed by deterministic business key. Quarantined records retain a reason. The raw Bronze layer remains available for audit.

# COMMAND ----------

save(spark.table(table("bronze_stores")).drop("ingested_at","run_id"),"silver_stores")
save(spark.table(table("bronze_products")).drop("ingested_at","run_id"),"silver_products")
s = spark.table(table("bronze_sales")).withColumn("date",F.expr("try_cast(date_raw AS DATE)"))
s = s.join(spark.table(table("silver_stores")).select("store_id").withColumn("known_store",F.lit(True)),"store_id","left")
s = s.join(spark.table(table("silver_products")).select("product_id").withColumn("known_product",F.lit(True)),"product_id","left")
s = s.withColumn("dq_reason", F.when(F.col("date").isNull(),"invalid_date")
 .when(F.col("known_store").isNull(),"unknown_store")
 .when(F.col("known_product").isNull(),"unknown_product")
 .when((F.col("sales_units")<0)|(F.col("demand_units")<0)|(F.col("unit_price")<0),"negative_value")
 .when(F.col("sales_units")>F.col("demand_units"),"sales_exceed_demand")
 .when(F.abs(F.col("sales_amount")-F.col("sales_units")*F.col("unit_price"))>0.011,"amount_mismatch"))
save(s.filter("dq_reason IS NOT NULL"),"silver_sales_quarantine")
clean = s.filter("dq_reason IS NULL").dropDuplicates(["record_id"]).select("record_id","date","store_id","product_id","demand_units","sales_units","unit_price","sales_amount","promo_flag")
save(clean.withColumn("sales_amount",F.col("sales_amount").cast("decimal(18,2)")),"silver_sales")
save(spark.table(table("bronze_inventory")).withColumn("date",F.to_date("date_raw")).drop("date_raw","ingested_at","run_id"),"silver_inventory")
save(spark.table(table("bronze_replenishment"))
 .withColumn("order_date",F.to_date("order_date_raw"))
 .withColumn("expected_date",F.to_date("expected_date_raw"))
 .drop("order_date_raw","expected_date_raw","ingested_at","run_id"),"silver_replenishment")
print("Silver tables and sales quarantine persisted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Demand forecasting with a time-based holdout
# MAGIC A transparent seasonal-naive baseline averages **only the preceding 56 training days by weekday** for each store/product. It generates one fixed-origin, 28-day holdout forecast. No holdout data enters training. Forecasts are benchmarked against synthetic unconstrained demand, not stock-constrained sales. This is a baseline, not an optimized ML forecasting system.
# MAGIC 
# MAGIC Forecast WAPE = sum absolute error / sum actual demand; bias = sum(forecast − actual) / sum actual. Ratios are computed from sums rather than averages of percentages.

# COMMAND ----------

cutoff = start + dt.timedelta(days=DAYS-28)
train_start = cutoff-dt.timedelta(days=56)
actuals = spark.table(table("silver_sales"))
model = actuals.filter((F.col("date")>=F.lit(train_start))&(F.col("date")<F.lit(cutoff))) .withColumn("weekday",F.dayofweek("date")).groupBy("store_id","product_id","weekday") .agg(F.avg("demand_units").alias("forecast_units"))
forecast = actuals.filter(F.col("date")>=F.lit(cutoff)).select("date","store_id","product_id","demand_units") .withColumn("weekday",F.dayofweek("date")).join(model,["store_id","product_id","weekday"],"left") .withColumn("forecast_origin",F.lit(cutoff-dt.timedelta(days=1))) .withColumn("absolute_error",F.abs(F.col("forecast_units")-F.col("demand_units"))) .withColumn("signed_error",F.col("forecast_units")-F.col("demand_units"))
save(forecast,"bronze_forecast")
save(forecast.withColumn("model_name",F.lit("56-day weekday mean; fixed 28-day holdout")),"silver_forecast")
print(f"Training: {train_start}–{cutoff-dt.timedelta(days=1)}; holdout: {cutoff}–{start+dt.timedelta(days=DAYS-1)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold reporting model (SQL over Delta)
# MAGIC `gold_fact_daily` has one date/store/product row and prevents sales/inventory fanout. Forecast and replenishment are separate facts at their own grains. Dimensions filter all applicable facts in Power BI using one-to-many, single-direction relationships.
# MAGIC 
# MAGIC **Availability:** share of store-product-days with opening + received stock > 0 (not an intraday shelf-availability measure). **Consumer demand fill:** sold / simulated demand. **Supplier fill:** shipped / requested on orders placed during the period (not receipt-date or on-time fill). **Stockout exposure:** lost demand × that day's synthetic selling price; this is modeled revenue exposure, not proven recoverable revenue. **Closing stock** is semi-additive: sum across products/stores at a date, never across dates.

# COMMAND ----------

sql_save("gold_dim_store",f"SELECT store_id, store_name, region FROM {table('silver_stores')}")
sql_save("gold_dim_product",f"SELECT product_id, sku, product_name, theme, list_price FROM {table('silver_products')}")
sql_save("gold_dim_date",f"""SELECT d AS date, year(d) AS year, month(d) AS month,
 date_format(d,'yyyy-MM') AS year_month, dayofweek(d) AS weekday
 FROM (SELECT explode(sequence(DATE '{start}',DATE '{start+dt.timedelta(days=DAYS-1)}',INTERVAL 1 DAY)) AS d)""")
sql_save("gold_fact_daily",f"""SELECT s.date,s.store_id,s.product_id,s.sales_units,s.demand_units,
 s.sales_amount,s.unit_price,s.promo_flag,i.opening_units,i.received_units,i.closing_units,
 s.demand_units-s.sales_units AS lost_units,
 CAST((s.demand_units-s.sales_units)*s.unit_price AS DECIMAL(18,2)) AS stockout_exposure,
 CASE WHEN i.opening_units+i.received_units>0 THEN 1 ELSE 0 END AS available_flag,
 CASE WHEN s.demand_units>s.sales_units THEN 1 ELSE 0 END AS stockout_flag
 FROM {table('silver_sales')} s JOIN {table('silver_inventory')} i
 USING(date,store_id,product_id)""")
sql_save("gold_fact_forecast",f"SELECT * FROM {table('silver_forecast')}")
sql_save("gold_fact_replenishment",f"SELECT * FROM {table('silver_replenishment')}")
sql_save("gold_sales_performance",f"""SELECT date,store_id,SUM(sales_amount) AS revenue,
 SUM(sales_units) AS units_sold,SUM(demand_units) AS demand_units
 FROM {table('gold_fact_daily')} GROUP BY date,store_id""")
sql_save("gold_inventory_availability",f"""SELECT date,store_id,COUNT(*) AS product_days,
 SUM(available_flag) AS available_product_days,SUM(stockout_flag) AS stockout_product_days,
 SUM(closing_units) AS closing_units,SUM(lost_units) AS lost_units,
 SUM(stockout_exposure) AS stockout_exposure
 FROM {table('gold_fact_daily')} GROUP BY date,store_id""")
sql_save("gold_forecast_performance",f"""SELECT store_id,product_id,SUM(demand_units) AS actual_units,
 SUM(forecast_units) AS forecast_units,SUM(absolute_error) AS absolute_error,
 SUM(signed_error) AS signed_error,SUM(absolute_error)/NULLIF(SUM(demand_units),0) AS wape,
 SUM(signed_error)/NULLIF(SUM(demand_units),0) AS bias
 FROM {table('gold_fact_forecast')} GROUP BY store_id,product_id""")
sql_save("gold_supplier_fill",f"""SELECT order_date,store_id,SUM(requested_units) AS requested_units,
 SUM(shipped_units) AS shipped_units,SUM(shipped_units)/NULLIF(SUM(requested_units),0) AS supplier_fill_rate
 FROM {table('gold_fact_replenishment')} GROUP BY order_date,store_id""")
print("Gold star schema and four reporting marts persisted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Automated quality gates and reconciliation
# MAGIC Any failed assertion stops the notebook before exports. Checks verify the deliberately injected defects, key uniqueness, expected completeness, referential integrity, inventory conservation, day-to-day stock continuity, bounds, forecast coverage, receipt reconciliation, and Bronze→Silver→Gold totals. Successful checks are logged to Delta; a failed execution remains visible in the notebook run output.

# COMMAND ----------

checks=[]
def check(name, actual, expected):
    ok = abs(float(actual)-float(expected)) < 0.00001
    checks.append((RUN_ID,name,str(actual),str(expected),bool(ok)))
    assert ok, f"FAILED {name}: {actual} != {expected}"
f = spark.table(table("gold_fact_daily"))
inv = spark.table(table("silver_inventory"))
po = spark.table(table("silver_replenishment"))
fc = spark.table(table("gold_fact_forecast"))
check("bronze_sales_rows",spark.table(table("bronze_sales")).count(),51848)
check("quarantined_rows",spark.table(table("silver_sales_quarantine")).count(),3)
check("duplicate_deliveries_removed",51848-3-clean.count(),5)
check("gold_daily_rows",f.count(),51840)
check("daily_key_duplicates",f.groupBy("date","store_id","product_id").count().filter("count != 1").count(),0)
check("inventory_balance",inv.filter("opening_units + received_units - closing_units < 0").count(),0)
check("inventory_conservation",f.filter("opening_units + received_units - sales_units != closing_units").count(),0)
w=Window.partitionBy("store_id","product_id").orderBy("date")
check("stock_continuity",inv.withColumn("previous_close",F.lag("closing_units").over(w)).filter("previous_close IS NOT NULL AND previous_close != opening_units").count(),0)
check("nonnegative_and_demand_bounds",f.filter("sales_units < 0 OR closing_units < 0 OR lost_units < 0 OR demand_units < sales_units").count(),0)
check("store_foreign_keys",f.join(spark.table(table("gold_dim_store")),"store_id","left_anti").count(),0)
check("product_foreign_keys",f.join(spark.table(table("gold_dim_product")),"product_id","left_anti").count(),0)
check("forecast_rows",fc.count(),8064)
check("forecast_nulls_or_negative",fc.filter("forecast_units IS NULL OR forecast_units < 0 OR forecast_origin >= date").count(),0)
check("forecast_key_duplicates",fc.groupBy("date","store_id","product_id").count().filter("count != 1").count(),0)
check("supplier_bounds",po.filter("shipped_units < 0 OR shipped_units > requested_units OR requested_units <= 0").count(),0)
last_date=start+dt.timedelta(days=DAYS-1)
check("receipts_reconcile",inv.agg(F.sum("received_units")).first()[0],po.filter(F.col("expected_date")<=F.lit(last_date)).agg(F.sum("shipped_units")).first()[0])
check("revenue_reconcile",f.agg(F.sum("sales_amount")).first()[0],clean.agg(F.sum("sales_amount")).first()[0])
check("sales_mart_reconcile",spark.table(table("gold_sales_performance")).agg(F.sum("revenue")).first()[0],f.agg(F.sum("sales_amount")).first()[0])
check_df=spark.createDataFrame(checks,"run_id string, check_name string, actual string, expected string, passed boolean")
check_df.withColumn("checked_at",F.current_timestamp()).write.format("delta").mode("append").saveAsTable(table("audit_quality_checks"))
display(check_df)
print(f"PASS: {len(checks)} quality gates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Runtime results and evidence
# MAGIC These results are computed by the executing Databricks environment, not pre-filled in this deliverable. Save/export the executed notebook with outputs after Run all completes. The Delta audit table keeps a run ID, row counts, elapsed time, and KPI snapshot. Re-running deterministic inputs should reproduce the same business metrics; runtime duration will vary.

# COMMAND ----------

metrics = spark.sql(f"""SELECT COUNT(*) AS store_product_days,
 SUM(sales_amount) AS revenue_usd,SUM(sales_units) AS units_sold,SUM(demand_units) AS demand_units,
 SUM(sales_units)/NULLIF(SUM(demand_units),0) AS consumer_demand_fill_rate,
 AVG(available_flag) AS daily_availability_rate,SUM(stockout_flag)/COUNT(*) AS stockout_day_rate,
 SUM(lost_units) AS lost_units,SUM(stockout_exposure) AS modeled_stockout_exposure_usd
 FROM {table('gold_fact_daily')}""").first().asDict()
metrics.update(spark.sql(f"""SELECT SUM(absolute_error)/NULLIF(SUM(demand_units),0) AS forecast_wape,
 SUM(signed_error)/NULLIF(SUM(demand_units),0) AS forecast_bias
 FROM {table('gold_fact_forecast')}""").first().asDict())
metrics.update(spark.sql(f"""SELECT SUM(shipped_units)/NULLIF(SUM(requested_units),0) AS supplier_fill_rate
 FROM {table('gold_fact_replenishment')}""").first().asDict())
metrics['ending_inventory_units']=f.filter(F.col('date')==F.lit(last_date)).agg(F.sum('closing_units')).first()[0]
metrics['run_id']=RUN_ID
metrics['quality_checks_passed']=len(checks)
metrics['elapsed_seconds']=round(time.time()-STARTED,2)
metrics['status']='SUCCEEDED'
metrics['data_classification']='SYNTHETIC; independent portfolio demonstration'
print(json.dumps(metrics,indent=2,default=str))
spark.createDataFrame([(RUN_ID,json.dumps(metrics,default=str))],"run_id string, metrics_json string") .withColumn("completed_at",F.current_timestamp()).write.format("delta").mode("append").saveAsTable(table("audit_runs"))
display(spark.sql(f"""SELECT d.region,SUM(f.sales_amount) AS revenue,
 SUM(f.lost_units) AS lost_units,SUM(f.stockout_exposure) AS modeled_exposure
 FROM {table('gold_fact_daily')} f JOIN {table('gold_dim_store')} d USING(store_id)
 GROUP BY d.region ORDER BY modeled_exposure DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Export to Power BI and capture the results
# MAGIC The next cell creates a ZIP containing Gold CSVs, runtime metrics, and quality-check evidence in a managed Unity Catalog Volume. This cell is optional: if volume creation is restricted, the executed Delta tables and notebook results remain usable. No personal data is exported. Download `LEGO_Demo_Executed_Outputs.zip` from **Catalog → your catalog → lego_portfolio_demo → exports → files**.
# MAGIC 
# MAGIC Import only the six `gold_dim_*` / `gold_fact_*` CSV files into Power BI for the star model. The four aggregate marts are provided for inspection; do not join them to the detailed facts. CSVs contain headers and ISO dates.

# COMMAND ----------

import os, zipfile, tempfile
spark.sql(f"CREATE VOLUME IF NOT EXISTS {NS}.`exports`")
volume_path=f"/Volumes/{CATALOG}/{SCHEMA}/exports"
export_tables=["gold_dim_store","gold_dim_product","gold_dim_date","gold_fact_daily","gold_fact_forecast","gold_fact_replenishment",
               "gold_sales_performance","gold_inventory_availability","gold_forecast_performance","gold_supplier_fill"]
zip_path=f"{volume_path}/LEGO_Demo_Executed_Outputs.zip"
# Driver collection is deliberate and safe only for this bounded 51,840-row demo.
with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as z:
    for name in export_tables:
        z.writestr(name+".csv",spark.table(table(name)).toPandas().to_csv(index=False))
    z.writestr("runtime_results.json",json.dumps(metrics,indent=2,default=str))
    z.writestr("quality_checks.csv",check_df.toPandas().to_csv(index=False))
print(f"Download from Catalog volume files: {zip_path}")
print(f"Export complete: {os.path.getsize(zip_path):,} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Power BI model and measures
# MAGIC Set IDs/quantities to Whole number; dates to Date; money to Fixed decimal number; forecast units/errors to Decimal number. Mark `gold_dim_date` as the date table. Use single-direction, one-to-many relationships:
# MAGIC 
# MAGIC | Dimension | Fact | Key |
# MAGIC |---|---|---|
# MAGIC | gold_dim_store | all three facts | store_id |
# MAGIC | gold_dim_product | all three facts | product_id |
# MAGIC | gold_dim_date | gold_fact_daily | date |
# MAGIC | gold_dim_date | gold_fact_forecast | date |
# MAGIC | gold_dim_date | gold_fact_replenishment | date → order_date |
# MAGIC 
# MAGIC Do not connect fact tables directly. `expected_date` is inactive unless using a separate role-playing arrival-date dimension. Reporting by order date and receipt date answers different questions.
# MAGIC 
# MAGIC Create measures from the supplied `PowerBI_Measures.dax`. Suggested pages: **Executive Overview** (revenue, consumer fill, availability, modeled exposure, regional performance); **Demand Forecast** (holdout actual vs forecast, WAPE, bias, theme/store filters); **Inventory & Supply** (stockout matrix, end-of-period stock, supplier fill). Label forecast charts “28-day retrospective holdout” and exposure “synthetic modeled exposure.”
# MAGIC 
# MAGIC ## 9. Automation and operational limits
# MAGIC The notebook is a sequential, rerunnable batch ETL pipeline with fail-fast checks, Delta persistence, quarantine, and run logging. It is **not scheduled automatically**. To demonstrate orchestration, use the notebook's Schedule control to create a manual-run Job if available in your Free Edition workspace, then inspect the Job run. Do not claim scheduled production operation without configuring and verifying it.
# MAGIC 
# MAGIC Bronze/Silver/Gold overwrite implements a repeatable full refresh for small synthetic inputs. A production extension would require incremental source ingestion, watermarks, Delta MERGE business keys, late-arriving-data policies, persisted quarantine resolution, schema contracts, access controls, monitoring, cost controls, and recovery testing. This prototype deliberately does not claim these are deployed.
# MAGIC 
# MAGIC For your executive portfolio, use only figures from `runtime_results.json` after a successful run. Describe outcomes as measured simulation results; do not claim actual LEGO revenue gains, real stockout reductions, or production integration.
