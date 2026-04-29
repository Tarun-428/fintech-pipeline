# Stock Market Real-Time Data Pipeline

A production-grade real-time data pipeline that collects stock prices from Binance, streams through Kafka, stores in Snowflake, and visualizes in Power BI dashboards - all orchestrated with Airflow.

```
Binance API → Kafka → Snowflake → Power BI Dashboard
                      ↓
                    Airflow (Scheduler)
```

---

## 📚 Documentation

Choose your starting point:

### 🚀 **New to this project?**
👉 Start here: [**QUICKSTART.md**](QUICKSTART.md)
- Get running in 15 minutes
- Step-by-step setup
- Minimal configuration

### 📖 **Complete Learning Guide**
👉 Read here: [**COMPLETE_GUIDE.md**](COMPLETE_GUIDE.md)
- Full project explanation
- How each component works
- Installation from scratch
- 18 comprehensive sections

### 🛠️ **Having Issues?**
👉 Check here: [**TROUBLESHOOTING_GUIDE.md**](TROUBLESHOOTING_GUIDE.md)
- 20+ common errors and fixes
- Docker issues
- Kafka problems
- Snowflake errors
- Power BI troubleshooting

### 📊 **Power BI Setup**
👉 See here: [**POWERBI_SETUP.md**](POWERBI_SETUP.md)
- Power BI configuration
- Dashboard creation
- Azure AD integration
- Auto-refresh setup

### 🎯 **Quick Reference**
👉 Use here: [**POWERBI_QUICKREF.md**](POWERBI_QUICKREF.md)
- Power BI cheat sheet
- Key components
- File reference

---

## 🏗️ Project Architecture

### Components

| Component | Purpose | Technology |
|-----------|---------|-----------|
| **Producer** | Collects live stock prices | Python + Binance API |
| **Message Queue** | Streams data in real-time | Apache Kafka |
| **Data Warehouse** | Stores structured data | Snowflake |
| **Scheduler** | Automates workflows | Apache Airflow |
| **Visualization** | Interactive dashboards | Power BI |
| **Orchestration** | Container management | Docker + Docker Compose |

### Data Flow

```
┌──────────────┐
│ Binance API  │ → BTCUSDT, ETHUSDT prices every 5 seconds
└────────┬─────┘
         │
         ▼
┌──────────────┐
│ Kafka Topic  │ → market_prices (streaming topic)
│ market_prices│
└────────┬─────┘
         │
         ▼
┌──────────────┐
│ Kafka Consumer
│ (Python)     │ → Reads messages, normalizes data
└────────┬─────┘
         │
         ▼
┌──────────────────────┐
│ Snowflake Table      │
│ MARKET_PRICES        │ → Stores 100,000+ records
│ (Cloud Data Warehouse│
└────────┬─────────────┘
         │
         ▼
┌──────────────┐
│ Power BI     │ → Beautiful dashboards
│ Dashboards   │
└──────────────┘
         ↑
         │
┌─────────────────┐
│ Airflow DAGs    │ → Automates refresh every hour
│ Scheduler       │
└─────────────────┘
```

---

## ⚡ Quick Start

```bash
# 1. Clone/Enter project
cd stock-market

# 2. Create Python environment
python3 -m venv venv
source venv/bin/activate

# 3. Install packages
pip install kafka-python requests snowflake-connector-python

# 4. Create .env with your Snowflake credentials
# See QUICKSTART.md

# 5. Start Docker services
docker-compose up -d

# 6. Run producer (collect prices)
python producer.py

# 7. Run consumer (load to Snowflake)
python fintech-airflow/dags/scripts/kafka_consumer.py

# 8. Check Snowflake for data!
# SELECT * FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES;
```

**Time to first data: ~15 minutes** ✅

---

## 📁 Project Structure

```
stock-market/
├── README.md                          # This file
├── QUICKSTART.md                      # Fast 15-min setup
├── COMPLETE_GUIDE.md                  # Full documentation (18 sections)
├── TROUBLESHOOTING_GUIDE.md           # Common errors & fixes
├── POWERBI_SETUP.md                   # Power BI detailed guide
├── POWERBI_QUICKREF.md                # Power BI quick reference
│
├── docker-compose.yml                 # Docker services (Kafka, Zookeeper, Postgres)
├── producer.py                        # Binance API → Kafka producer
├── .env                               # Your secrets (Snowflake credentials)
├── .env.powerbi.template              # Power BI config template
│
└── fintech-airflow/                   # Airflow project
    ├── dags/
    │   ├── fintech_pipeline.py        # Main ETL workflow
    │   ├── powerbi_dag.py             # Power BI refresh workflow
    │   └── scripts/
    │       ├── kafka_consumer.py      # Kafka → Snowflake connector
    │       ├── powerbi_config.py      # Power BI auth config
    │       └── powerbi_setup.py       # Power BI initialization
    ├── requirements.txt               # Python packages
    ├── Dockerfile                     # Docker image
    └── airflow_settings.yaml          # Airflow config
```

---

## 🎯 What You'll Learn

By working through this project, you'll understand:

- ✅ **Real-time Data Pipelines** - How to stream data at scale
- ✅ **Kafka Architecture** - Message queues and distributed streaming
- ✅ **Cloud Data Warehouses** - Snowflake for analytics
- ✅ **Workflow Orchestration** - Airflow for automation
- ✅ **API Integration** - Binance API for live data
- ✅ **Docker Containerization** - Running services in containers
- ✅ **Business Intelligence** - Power BI dashboards
- ✅ **Python** - Building data pipelines

---

## 🚀 Technologies Used

| Technology | Purpose | Version |
|-----------|---------|---------|
| Python | Main language | 3.8+ |
| Docker | Containers | 20+ |
| Docker Compose | Multi-container | 2+ |
| Apache Kafka | Message streaming | 7.4.0 |
| Snowflake | Data warehouse | Current |
| Apache Airflow | Workflow scheduler | 2.5+ |
| Power BI | Visualization | Latest |
| MSAL | Azure authentication | 1.20+ |

---

## 📋 Prerequisites

Before starting, you need:

1. **Docker** - Install from https://www.docker.com/
2. **Python 3.8+** - Install from https://www.python.org/
3. **Snowflake Account** - Free trial at https://signup.snowflake.com
4. **Basic Terminal Knowledge** - Comfortable with `bash` or `cmd`
5. **Optional: Power BI Account** - For dashboards (https://powerbi.microsoft.com)

---

## 🐛 Troubleshooting

Stuck? Check the [**TROUBLESHOOTING_GUIDE.md**](TROUBLESHOOTING_GUIDE.md) for:

- Docker container issues
- Kafka connection problems
- Snowflake authentication errors
- Python package missing errors
- Port already in use issues
- Power BI connectivity
- And 20+ other common issues!

---

## 📖 Documentation Files

| File | Purpose | Read Time |
|------|---------|-----------|
| [QUICKSTART.md](QUICKSTART.md) | Fast setup guide | 5-10 min |
| [COMPLETE_GUIDE.md](COMPLETE_GUIDE.md) | Full learning guide | 30-45 min |
| [TROUBLESHOOTING_GUIDE.md](TROUBLESHOOTING_GUIDE.md) | Error solutions | 20-30 min |
| [POWERBI_SETUP.md](POWERBI_SETUP.md) | Power BI guide | 15-20 min |
| [POWERBI_QUICKREF.md](POWERBI_QUICKREF.md) | Power BI reference | 5-10 min |

---

## 🎓 Learning Path

### For Beginners:
1. Read [QUICKSTART.md](QUICKSTART.md)
2. Get it running
3. Read relevant sections of [COMPLETE_GUIDE.md](COMPLETE_GUIDE.md)
4. Play with dashboards in Power BI

### For Experienced Developers:
1. Quick scan of [QUICKSTART.md](QUICKSTART.md)
2. Review [COMPLETE_GUIDE.md](COMPLETE_GUIDE.md) architecture sections
3. Check [TROUBLESHOOTING_GUIDE.md](TROUBLESHOOTING_GUIDE.md) for edge cases
4. Customize and extend

---

## 💡 Key Commands

```bash
# Docker
docker-compose up -d           # Start all services
docker ps                      # Check running containers
docker logs kafka              # View Kafka logs
docker-compose down            # Stop all services

# Python
source venv/bin/activate       # Activate virtual environment
python producer.py             # Run stock price producer
pip install -r requirements.txt # Install packages

# Kafka (inside Docker)
docker exec kafka bash
kafka-topics --list
kafka-console-consumer --topic market_prices

# Airflow
astro dev start                # Start with Astronomer
airflow dags list              # List DAGs
airflow dags trigger <dag>     # Run manually

# Snowflake
# Login at https://snowflake.com
SELECT * FROM FINTECH_ANALYTICS.RAW.MARKET_PRICES;
```

---

## 🔒 Security Notes

⚠️ **Important:**
- Never commit `.env` file to Git (has passwords!)
- Add `.env` to `.gitignore`
- Use strong passwords for Snowflake
- Don't share Power BI credentials
- In production, use secrets manager (AWS Secrets, Vault, etc.)

---

## 🤝 Contributing

This is a learning project! Feel free to:
- Fork and modify
- Add more data sources
- Improve documentation
- Add more visualizations
- Contribute improvements

---

## 📚 References

- **Kafka Documentation**: https://kafka.apache.org/documentation/
- **Snowflake Documentation**: https://docs.snowflake.com/
- **Apache Airflow**: https://airflow.apache.org/docs/
- **Power BI**: https://docs.microsoft.com/power-bi/
- **Docker**: https://docs.docker.com/

---

## 📝 Project Status

✅ **Complete and Working**
- Producer: Collecting prices from Binance
- Kafka: Streaming in real-time
- Snowflake: Storing data
- Airflow: Automating workflows
- Power BI: Creating dashboards

---

## 📧 Support

Having issues? Check these in order:
1. [QUICKSTART.md](QUICKSTART.md) - Basic setup
2. [COMPLETE_GUIDE.md](COMPLETE_GUIDE.md) - Deep explanations
3. [TROUBLESHOOTING_GUIDE.md](TROUBLESHOOTING_GUIDE.md) - Error solutions
4. Official documentation (links above)

---

## 🎉 You're All Set!

Pick a documentation file and start learning:

**[→ Start with QUICKSTART.md (5-15 min)](QUICKSTART.md)**

or

**[→ Start with COMPLETE_GUIDE.md (comprehensive)](COMPLETE_GUIDE.md)**

---

**Last Updated**: April 2024
**Version**: 1.0
**Status**: ✅ Production Ready
