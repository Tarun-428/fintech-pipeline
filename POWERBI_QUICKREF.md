# Power BI Integration - Quick Reference

## What Was Added

### Files Created

1. **`dags/scripts/powerbi_config.py`**
   - Azure AD authentication
   - Power BI credentials management
   - Snowflake connection configuration

2. **`dags/scripts/powerbi_setup.py`**
   - Dataset creation and initialization
   - Dashboard JSON configuration generator
   - Power BI manager class for API interactions

3. **`dags/powerbi_dag.py`**
   - Automated Airflow DAG for hourly dashboard refresh
   - Tasks: setup, refresh, validate, summarize

4. **`POWERBI_SETUP.md`** (Detailed guide)
   - Complete setup instructions
   - Dashboard visualization details
   - Troubleshooting guide
   - Performance tuning tips

5. **`.env.powerbi.template`**
   - Environment variable template
   - Configuration reference

### Dependencies Added to `requirements.txt`

```
powerbi-client>=1.3.0    # Power BI REST API client
msal>=1.20.0            # Microsoft authentication
```

## Quick Start

### 1️⃣ Setup Azure AD (5 minutes)
- Register app in Azure AD portal
- Create client secret
- Get Tenant ID, Client ID, Client Secret

### 2️⃣ Configure Environment (2 minutes)
```bash
cp .env.powerbi.template .env.powerbi
# Edit .env.powerbi with your credentials
```

### 3️⃣ Initialize Power BI (1 minute)
```bash
python fintech-airflow/dags/scripts/powerbi_setup.py
```

### 4️⃣ Create Snowflake Datasource in Power BI Desktop (5 minutes)
- Get Data → Snowflake
- Server: `your-account.snowflakecomputing.com`
- Database: `FINTECH_ANALYTICS`

### 5️⃣ Build Dashboards (10 minutes)
Use the JSON config from `powerbi_setup.py` output:
- Line chart: Price trends
- Bar chart: Price by symbol
- Card: Message count
- Table: Latest prices
- Pie chart: Partition distribution

### 6️⃣ Enable Automatic Refresh (1 minute)
- Publish to Power BI Service
- DAG `powerbi_dashboard_refresh` runs hourly
- Validates and refreshes dashboards automatically

## Key Components

### powerbi_config.py
```python
PowerBIConfig.get_access_token()              # Get Azure AD token
PowerBIConfig.get_snowflake_connection_string()  # Snowflake DSN
```

### powerbi_setup.py
```python
manager = PowerBIManager()
manager.create_market_prices_dataset()        # Dataset schema
manager.create_dashboard_json()               # Dashboard config
manager.setup_power_bi()                      # Full setup
```

### powerbi_dag.py
```
setup_powerbi 
    ↓
refresh_market_prices_dataset 
    ↓
validate_dashboard_data 
    ↓
generate_dashboard_summary
```

## Architecture Flow

```
Kafka Topic "market_prices"
    ↓ (kafka_consumer.py)
Snowflake Table "FINTECH_ANALYTICS.RAW.MARKET_PRICES"
    ↓ (Power BI Connector)
Power BI Dataset "Market Prices Dataset"
    ↓ (Power BI Desktop/Service)
Dashboards & Reports (Interactive Visualizations)
    ↓ (Scheduled by Airflow)
Hourly Refresh (stays current)
```

## Environment Variables Needed

| Variable | Example | Source |
|----------|---------|--------|
| `POWERBI_TENANT_ID` | `abc123...` | Azure Portal → Azure AD |
| `POWERBI_CLIENT_ID` | `def456...` | Azure AD App Registration |
| `POWERBI_CLIENT_SECRET` | `xyz789...` | Azure AD Client Secret |
| `POWERBI_WORKSPACE_ID` | `ghi012...` | Power BI Service Settings |
| `SNOWFLAKE_ACCOUNT` | `ab12345.us-east-1` | Snowflake Account |
| `SNOWFLAKE_USER` | `powerbi_user` | Your Snowflake user |
| `SNOWFLAKE_PASSWORD` | `****` | Your Snowflake password |

## Troubleshooting Checklist

- [ ] Azure AD app registered and secret created
- [ ] All environment variables configured
- [ ] Kafka consumer running and populating Snowflake
- [ ] Snowflake connection works: `snowflake_account.snowflakecomputing.com`
- [ ] Power BI setup script runs without errors
- [ ] Power BI Desktop Snowflake connector working
- [ ] Dashboard visualizations show sample data
- [ ] Published to Power BI Service
- [ ] Airflow DAG deployed and enabled

## API Refresh (Advanced)

Power BI datasets can be programmatically refreshed:

```python
from powerbi_config import PowerBIConfig
import requests

token = PowerBIConfig.get_access_token()
workspace_id = PowerBIConfig.WORKSPACE_ID
dataset_id = "your-dataset-id"

headers = {"Authorization": f"Bearer {token}"}
url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"

response = requests.post(url, headers=headers)
```

## Next: Integration with Alerts

To extend this setup, consider:
1. **Email alerts** when data is stale
2. **Slack notifications** on dashboard refresh failures
3. **Data quality checks** before refresh
4. **Performance monitoring** with Power BI Premium

## Files Summary

| File | Purpose | Run Frequency |
|------|---------|---------------|
| `powerbi_config.py` | Config & auth | On-demand |
| `powerbi_setup.py` | Initial setup | Once |
| `powerbi_dag.py` | Auto-refresh | Hourly (Airflow) |

---

For complete details, see [POWERBI_SETUP.md](POWERBI_SETUP.md)
