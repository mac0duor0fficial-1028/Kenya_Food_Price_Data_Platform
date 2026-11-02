# Power BI project (PBIP)

`KenyaFoodPrices.pbip` = TMDL semantic model + PBIR report, text-only and git-friendly.

## Open it
1. Power BI Desktop → File → Options → Preview features → enable **Power BI Project (.pbip) save option** and **Store semantic model using TMDL format**; restart.
2. Make sure the warehouse has the views in `sql/005_powerbi_views.sql`
   (`psql postgresql://rtfp:rtfp@localhost:5433/rtfp -f sql/005_powerbi_views.sql`) and that at least one pipeline run has been **published**.
3. Open `KenyaFoodPrices.pbip`. Install the Npgsql provider if Desktop asks.
4. Home → Transform data → Edit parameters: `PgServer` (`localhost:5433`), `PgDatabase` (`rtfp`). Refresh and enter the database credentials when prompted.

## Model
| Table | Source | Notes |
|---|---|---|
| `fact_food_price` | `dw.v_fact_food_price_published` | published run only; keys hidden |
| `dim_date` | `dw.dim_date` | marked as date table on `month_start` |
| `dim_market` | `dw.dim_market` | lat/lon categorised |
| `dim_product` | `dw.dim_product` | |
| `lineage` | `dw.v_lineage` | run history |

Relationships (many-to-one, single direction): fact → `dim_date[month_start]`, `dim_market[market_key]`, `dim_product[product_key]`.

Measures (on `fact_food_price`): Avg Price, Avg Price Prev Year, YoY %, Price Index (base 100), Markets Reporting, Products Reporting, Price Records, Latest Month, Latest Avg Price. On `lineage`: Rows Loaded (latest run).

Pages: Overview, Markets, Products, Lineage.

Prices of different products have different units: always filter to one product (or use the price index) before reading an average.
