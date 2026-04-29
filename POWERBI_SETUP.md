# Power BI Integration Setup Guide

## Overview

This integration connects your real-time market price data from Kafka → Snowflake → Power BI, enabling interactive dashboards and analytics.

## Architecture

```
Kafka (Market Prices)
    ↓
Snowflake (RAW.MARKET_PRICES)
    ↓
Power BI Connector
    ↓
Dashboards & Reports
```

## Prerequisites

1. **Power BI Account**: Power BI Pro or Premium license
2. **Azure AD**: Tenant with app registration
3. **Snowflake Connection**: Already configured in kafka_consumer.py
4. **Airflow**: Running fintech-airflow environment

## Setup Steps

### 1. Azure AD App Registration

Register an application in Azure AD for Power BI API access:

```bash
# Using Azure CLI
az ad app create --display-name "PowerBI-Fintech-Integration"
az ad app credential create --id <APPLICATION_ID>
```

Or use Azure Portal:
- Go to Azure AD → App registrations
- Create new application
- Generate client secret
- Grant permissions: Power BI Service → Dataset.ReadWrite.All

### 2. Environment Variables

Add to your `.env` file or docker-compose.yml:

```bash
# Azure AD Configuration
POWERBI_TENANT_ID=your-tenant-id
POWERBI_CLIENT_ID=your-client-id
POWERBI_CLIENT_SECRET=your-client-secret
POWERBI_USERNAME=your-powerbi-user@yourdomain.com
POWERBI_PASSWORD=your-powerbi-password

# Power BI Configuration
POWERBI_WORKSPACE_ID=your-workspace-id  # OR use GROUP_ID
POWERBI_GROUP_ID=your-group-id

# Snowflake Configuration
SNOWFLAKE_SERVER=your-snowflake-server.snowflakecomputing.com
SNOWFLAKE_DATABASE=FINTECH_ANALYTICS
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_MARKET_PRICES_TABLE=MARKET_PRICES
```

### 3. Install Python Dependencies

Dependencies are already added to `requirements.txt`:
- `powerbi-client>=1.3.0` - Power BI API client
- `msal>=1.20.0` - Microsoft authentication library

Install via pip or Airflow:
```bash
pip install -r fintech-airflow/requirements.txt
```

### 4. Initialize Power BI Setup

Run the setup script to create initial dataset configuration:

```bash
python fintech-airflow/dags/scripts/powerbi_setup.py
```

This will:
- Create Market Prices dataset schema
- Generate dashboard JSON configuration
- Validate Power BI authentication

### 5. Create Power BI Datasource

In Power BI Desktop:

1. **Get Data → Snowflake**
2. **Server**: `<SNOWFLAKE_SERVER>`
3. **Database**: `FINTECH_ANALYTICS`
4. **Import Mode**: DirectQuery (recommended for real-time data)

### 6. Import Dashboard Configuration

The setup script generates a dashboard JSON at `/tmp/powerbi_dashboard.json`.

**Option A: Manual Import (Recommended)**
1. Open Power BI Desktop
2. Create new report
3. Connect to Snowflake datasource
4. Create visualizations based on the JSON configuration:
   - **Average Price Trend**: Line chart (Time-series)
   - **Price by Symbol**: Bar chart
   - **Total Messages**: Card visual
   - **Latest Prices**: Table
   - **Partition Distribution**: Pie chart

**Option B: Programmatic Import**
Use Power BI REST API to import the dashboard (requires Power BI Premium).

### 7. Publish to Power BI Service

In Power BI Desktop:
1. **File → Publish**
2. Select workspace
3. Configure dataset refresh schedule (recommended: hourly)

### 8. Configure Automatic Refresh

The `powerbi_dag.py` DAG handles automated refreshes:

- **Refresh Schedule**: Hourly (adjustable in `schedule_interval`)
- **Tasks**:
  - `setup_powerbi`: Ensure configuration is current
  - `refresh_market_prices_dataset`: Trigger Power BI dataset refresh
  - `validate_dashboard_data`: Verify Snowflake data freshness
  - `generate_dashboard_summary`: Log metrics and alerts

## Dashboard Visualizations

### 1. Average Price Trend
- **Type**: Line chart
- **X-axis**: EVENT_TIME (hourly)
- **Y-axis**: Average PRICE
- **Use Case**: Monitor price movements over time

### 2. Price by Symbol
- **Type**: Bar chart
- **Categories**: SYMBOL
- **Values**: Average PRICE
- **Use Case**: Compare prices across different symbols

### 3. Total Messages Ingested
- **Type**: Card visual
- **Value**: COUNT(*)
- **Use Case**: Monitor data pipeline health

### 4. Latest Prices Table
- **Type**: Table
- **Columns**: SYMBOL, PRICE, EVENT_TIME, INGESTED_AT
- **Use Case**: See most recent market data

### 5. Kafka Partition Distribution
- **Type**: Pie chart
- **Categories**: KAFKA_PARTITION
- **Values**: COUNT(*)
- **Use Case**: Monitor data distribution across Kafka partitions

## Monitoring & Alerts

### Dashboard Health Checks

Monitor the following metrics:

1. **Data Freshness**: How recent is the latest market price?
2. **Record Count**: Total messages processed
3. **Ingestion Rate**: Messages per hour
4. **Snowflake Query Performance**: Response time for dashboard queries

### Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| **"No data in dashboard"** | Check Snowflake connection, verify KAFKA_CONSUMER has processed messages |
| **"Slow dashboard load"** | Use DirectQuery mode, add aggregations in Power BI, optimize Snowflake queries |
| **"Refresh failed"** | Verify Snowflake credentials, check Power BI service logs |
| **"Connection timeout"** | Verify firewall rules, check Snowflake warehouse status |

## Advanced Configuration

### Custom Measures

Add these DAX measures to your Power BI model:

```dax
// Price change percentage
Price Change % = 
DIVIDE(
    MAX(MARKET_PRICES[PRICE]) - MIN(MARKET_PRICES[PRICE]),
    MIN(MARKET_PRICES[PRICE]),
    0
)

// Messages per hour
Messages Per Hour = 
DIVIDE(
    COUNTA(MARKET_PRICES[ID]),
    COUNTA(DISTINCT(MARKET_PRICES[EVENT_TIME])),
    0
)

// Symbols monitored
Unique Symbols = DISTINCTCOUNT(MARKET_PRICES[SYMBOL])
```

### Row-Level Security (RLS)

To restrict data by user:
1. Create a dimension table with user/symbol mappings
2. Define RLS rules in Power BI: `SYMBOLID() = [Symbol]`
3. Publish with RLS enabled

### Scheduled Refresh API

For Power BI Premium, trigger refreshes via API:

```python
from powerbi_config import PowerBIConfig

token = PowerBIConfig.get_access_token()
# POST /groups/{workspace_id}/datasets/{dataset_id}/refreshes
```

## Troubleshooting

### Step 1: Check Airflow DAG Status
```bash
airflow dags list
airflow dags test powerbi_dashboard_refresh
```

### Step 2: Verify Snowflake Data
```sql
SELECT COUNT(*), MAX(INGESTED_AT) FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES;
```

### Step 3: Test Power BI Connection
In Power BI Desktop:
- **Data → Get Data → Options → Power BI Connector**
- Verify token refresh works

### Step 4: Check Logs
```bash
# Airflow logs
docker logs <airflow-container>

# Power BI setup script
python -u dags/scripts/powerbi_setup.py
```

## Performance Tuning

1. **Aggregation Tables**: Pre-aggregate hourly data in Snowflake
2. **Incremental Refresh**: Use `INGESTED_AT` to refresh only new data
3. **Cache Strategy**: Enable Power BI Query Folding
4. **Snowflake Compute**: Scale warehouse for peak hours

## Security Best Practices

1. ✅ Store credentials in environment variables or secrets manager
2. ✅ Use service principal (app registration) instead of user credentials
3. ✅ Enable Row-Level Security in Power BI
4. ✅ Audit Power BI API calls in Azure AD
5. ✅ Restrict Snowflake role permissions to minimum needed

## Next Steps

- [ ] Register Azure AD application
- [ ] Set environment variables
- [ ] Run `powerbi_setup.py` script
- [ ] Create Snowflake datasource in Power BI Desktop
- [ ] Build dashboard visualizations
- [ ] Publish to Power BI Service
- [ ] Enable scheduled refresh in Airflow
- [ ] Share dashboard with stakeholders

## Support & Documentation

- **Power BI REST API**: https://docs.microsoft.com/power-bi/developer/rest-api
- **Snowflake Connector**: https://docs.snowflake.com/en/user-guide/
- **MSAL Python**: https://github.com/AzureAD/microsoft-authentication-library-for-python
- **Airflow Docs**: https://airflow.apache.org/docs/

---

For issues or questions, check logs and configuration first, then consult the troubleshooting section.
