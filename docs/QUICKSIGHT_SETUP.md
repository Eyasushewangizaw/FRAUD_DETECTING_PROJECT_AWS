## QuickSight setup for the fraud detection dashboard

This guide walks through connecting **Amazon QuickSight** to your **Redshift** fraud mart and building a basic fraud dashboard with four core visuals.

You will need:

- An AWS account with QuickSight enabled
- A running Amazon Redshift (or Redshift Serverless) endpoint with the `fraud.fact_transactions` table populated
- Network access from QuickSight to Redshift (VPC / security groups configured)

---

## 1. Sign up for QuickSight (Standard edition)

1. Sign in to the **AWS Management Console** in the region where you want to host QuickSight (for example `us-east-1`).
2. In the search bar, type **“QuickSight”** and open **Amazon QuickSight**.
3. If this is your first time:
   - Choose **Sign up for QuickSight**.
   - Select **Standard Edition**.
   - Choose an **account name** (for example `fraud-analytics`).
   - Choose the **region** (match your Redshift region when possible).
   - Provide an **email address** for QuickSight notifications.
   - Choose whether to allow access to **Amazon S3** and **Redshift** (enable both).
4. Confirm and finish the signup process. QuickSight will provision your account (this can take a few minutes).

---

## 2. Create a Redshift data source (manual connection)

In QuickSight:

1. From the QuickSight home page, choose **Datasets** in the left navigation.
2. Click **New dataset**.
3. Under **From new data sources**, choose **Redshift**.
4. Fill in the **data source details**:
   - **Data source name**: `fraud-redshift` (or any descriptive name).
   - **Connection type**: usually **Standard** (unless you’re using Redshift Serverless + IAM or a private VPC-only setup).
   - **Hostname**: your Redshift endpoint host name (e.g. `fraud-cluster.abcdefg123.us-east-1.redshift.amazonaws.com`).
   - **Port**: typically `5439` (or your custom Redshift port).
   - **Database name**: your analytics DB name (the one containing the `fraud` schema).
   - **Username**: a DB user with `SELECT` on `fraud.fact_transactions`.
   - **Password**: that user’s password.
5. Optionally, choose **VPC** and **subnet / security groups** if QuickSight must connect to Redshift in a private VPC (Enterprise features).
6. Click **Create data source**. QuickSight tests the connection and then prompts you to choose tables.

---

## 3. Create a dataset from `fraud.fact_transactions`

1. After the data source is created, choose **Use custom SQL** or **Select tables**:
   - The simplest is **Select tables**.
2. In the schema list, select **`fraud`**.
3. Select the table **`fact_transactions`**.
4. Click **Edit/Preview data**.
5. Verify that the key columns are present:
   - `transaction_id`, `amount`, `transaction_timestamp`, `is_international`, `fraud_score`, `is_fraud_flagged`, `merchant_category`, `transaction_country`, etc.
6. In the **Fields list**, set the following field types if QuickSight did not infer them correctly:
   - `amount` → **Number** (decimal).
   - `fraud_score` → **Number** (decimal).
   - `transaction_timestamp` → **Date** or **DateTime** with the correct format (for example `yyyy-MM-dd HH:mm:ss` or ISO 8601).
   - `is_fraud_flagged` and `is_international` → **Boolean** or **Categorical** (as appropriate).
7. Click **Save & visualize** to create the dataset and open a new analysis.

---

## 4. Build the dashboard visuals

You’ll now be in a **new analysis** with the `fraud.fact_transactions` dataset selected.

### 4.1 Line chart: daily transactions vs fraud count over time

Business question: *How are overall transactions and fraud counts trending by day?*

1. In the top-left, choose **Add → Add visual**.
2. In the **Visual types** pane, choose **Line chart**.
3. In the **Fields list**, drag:
   - `transaction_timestamp` to **X axis**.
4. On the field well for `transaction_timestamp`:
   - Click the field drop-down → choose **Aggregate** by **Day** (or **Trunc to day**).
5. Drag:
   - `transaction_id` to **Y axis** and change **Aggregate** to **Count** (or **Distinct count** if desired).
   - `is_fraud_flagged` to **Color** as a filter is not enough:  
     - Alternatively, create a **calculated field** `fraud_flag_int` = `ifelse(is_fraud_flagged, 1, 0)`:
       - **Fields list → + Add → Add calculated field**
       - Name: `fraud_flag_int`
       - Formula: `ifelse({is_fraud_flagged}, 1, 0)`
     - Then drag `fraud_flag_int` to **Y axis** as a second measure, aggregated as **Sum**.
6. Ensure:
   - `transaction_id (count)` is labeled **Total transactions**.
   - `fraud_flag_int (sum)` is labeled **Fraud count**.
7. Optionally enable a **dual axis** (if supported in your edition) or keep two lines in one axis.

### 4.2 KPI cards: total transactions, fraud count, fraud rate %, total fraud $

Business question: *What are my headline fraud KPIs right now?*

You will build a few **Key performance indicator** visuals.

1. Click **Add → Add visual**.
2. Choose the **KPI** visual type.
3. For **Total transactions**:
   - Drag `transaction_id` to **Value** → set aggregate to **Count**.
   - Optionally set a filter to a date window (e.g. last 30 days).
   - Rename the visual title to **Total transactions**.
4. For **Fraud count**:
   - Duplicate the KPI visual or add a new one.
   - Use the **`fraud_flag_int`** calculated field as **Value**, with **Sum** aggregate.
   - Rename the visual to **Fraud count**.
5. For **Fraud rate %**:
   - Create a new **calculated field** `fraud_rate_pct` with formula:  
     `100 * sum(ifelse({is_fraud_flagged}, 1, 0)) / sum(1)`  
     (or `100 * sum({fraud_flag_int}) / count({transaction_id})` depending on your preference).
   - Add another KPI visual and drag `fraud_rate_pct` to **Value**.
   - Set format to **Percentage with 2 decimals**.
   - Title it **Fraud rate %**.
6. For **Total fraud USD**:
   - Create a calculated field `fraud_amount` = `ifelse({is_fraud_flagged}, {amount}, 0)` if you want to avoid filtering.
   - New KPI visual with `fraud_amount` as **Value** (aggregate **Sum**).
   - Format as **Currency** (USD) and title **Total fraud $**.

You can arrange the four KPI cards in a row at the top of the dashboard.

### 4.3 Bar chart: fraud incidents by merchant category

Business question: *Which merchant categories drive the most fraud incidents and exposure?*

1. Add a new visual → choose **Vertical bar chart**.
2. Drag `merchant_category` to **X axis**.
3. Drag `fraud_flag_int` (or `is_fraud_flagged`) to **Y axis**:
   - Use `fraud_flag_int` as **Sum** for incident count, or  
   - Use `is_fraud_flagged` with aggregate **Count** while adding a filter for `is_fraud_flagged = true`.
4. Optionally drag `amount` to **Tooltip** with aggregate **Sum** to show total fraud USD per category.
5. Sort by **Y axis descending** to highlight worst offenders at the left.

### 4.4 Pie chart: international vs domestic fraud split

Business question: *What portion of fraud is international vs domestic?*

1. Add a new visual → choose **Pie chart**.
2. If you want to focus **only on fraud cases**:
   - Add a **filter** on `is_fraud_flagged = true`.
3. Drag `is_international` to **Group/Color**.
4. Drag `fraud_flag_int` (or `transaction_id`) to **Value** with:
   - `fraud_flag_int` as **Sum**, or
   - `transaction_id` with **Count** and filter `is_fraud_flagged = true`.
5. Optionally, re-label `is_international` values in the legend using a calculated field:
   - `intl_label = ifelse({is_international}, 'International', 'Domestic')`
   - Use `intl_label` instead of `is_international` for grouping.

---

## 5. Publish the dashboard and share it

Once your visuals look good:

1. Click **Share → Publish dashboard** in the top-right of the analysis.
2. Provide a **Dashboard name**, e.g. **Fraud Detection Overview**.
3. Choose whether to **create a new dashboard** or **replace an existing one**.
4. Click **Publish dashboard**.
5. After publishing, you’ll see a **Share** button:
   - Choose **Share dashboard**.
   - Add specific **QuickSight users** or **groups** within your QuickSight account.
   - Optionally enable **“share link”** within your organization (depending on your governance policies).
6. Save.

Users will now be able to access the dashboard from their QuickSight home → **Dashboards**.

---

## 6. Set up dashboard refresh on a schedule

QuickSight **refreshes datasets**, not dashboards directly. To keep your fraud dashboard fresh:

1. From the left navigation, go to **Datasets**.
2. Click the dataset you created for `fraud.fact_transactions`.
3. Choose **Schedule refresh** (or **Manage refresh**).
4. Configure a **Refresh schedule**:
   - **Frequency**: align with your Redshift load and pipeline; for an hourly Airflow run, you might set **Hourly** with a delay (e.g. 10–15 minutes after the top of the hour) to leave time for ETL and COPY.
   - **Time / Time zone**: choose a time window that trails your DAG completion.
5. Save the schedule.

From now on:

- Airflow/Glue/Redshift keep `fraud.fact_transactions` current.
- QuickSight refreshes the dataset on the schedule you defined.
- The **Fraud Detection Overview** dashboard will show up-to-date metrics for business users.

