#!/usr/bin/env python3
"""
Power BI dataset and report initialization.
Creates Power BI datasets connected to Snowflake data.
"""
import json
import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any

from powerbi_config import PowerBIConfig


class PowerBIManager:
    """Manages Power BI datasets, reports, and dashboards."""
    
    def __init__(self):
        """Initialize Power BI manager with API credentials."""
        try:
            PowerBIConfig.validate_config()
            self.access_token = PowerBIConfig.get_access_token()
            self.workspace_id = PowerBIConfig.WORKSPACE_ID or PowerBIConfig.GROUP_ID
            
            if not self.workspace_id:
                raise ValueError("Either POWERBI_WORKSPACE_ID or POWERBI_GROUP_ID must be set")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Power BI Manager: {e}")
    
    def create_market_prices_dataset(self) -> Dict[str, Any]:
        """
        Create a Power BI dataset for market prices from Snowflake.
        
        Returns:
            Dataset configuration
        """
        dataset_config = {
            "name": "Market Prices Dataset",
            "tables": [
                {
                    "name": "MARKET_PRICES",
                    "columns": [
                        {"name": "ID", "dataType": "Int64"},
                        {"name": "SYMBOL", "dataType": "String"},
                        {"name": "PRICE", "dataType": "Double"},
                        {"name": "EVENT_TIME", "dataType": "DateTime"},
                        {"name": "INGESTED_AT", "dataType": "DateTime"},
                        {"name": "KAFKA_TOPIC", "dataType": "String"},
                        {"name": "KAFKA_PARTITION", "dataType": "Int32"},
                        {"name": "KAFKA_OFFSET", "dataType": "Int64"},
                        {"name": "PAYLOAD", "dataType": "String"},
                    ],
                    "measures": [
                        {
                            "name": "Average Price",
                            "expression": "AVERAGEX(MARKET_PRICES, MARKET_PRICES[PRICE])"
                        },
                        {
                            "name": "Max Price",
                            "expression": "MAXX(MARKET_PRICES, MARKET_PRICES[PRICE])"
                        },
                        {
                            "name": "Min Price",
                            "expression": "MINX(MARKET_PRICES, MARKET_PRICES[PRICE])"
                        },
                        {
                            "name": "Message Count",
                            "expression": "COUNTA(MARKET_PRICES[ID])"
                        }
                    ]
                }
            ],
            "relationships": [],
            "expressions": []
        }
        return dataset_config
    
    def create_dashboard_json(self) -> str:
        """
        Generate JSON configuration for Power BI dashboard.
        
        Returns:
            JSON string with dashboard configuration
        """
        dashboard_config = {
            "name": "Market Prices Dashboard",
            "description": "Real-time market price analytics from Kafka/Snowflake",
            "tiles": [
                {
                    "title": "Average Price Trend",
                    "type": "LineChart",
                    "visualKey": "avg_price_trend",
                    "axis_x": "EVENT_TIME",
                    "axis_y": "PRICE",
                    "query": "SELECT EVENT_TIME, AVG(CAST(PRICE AS FLOAT)) as PRICE FROM MARKET_PRICES GROUP BY DATE(EVENT_TIME) ORDER BY EVENT_TIME DESC LIMIT 100"
                },
                {
                    "title": "Price by Symbol",
                    "type": "BarChart",
                    "visualKey": "price_by_symbol",
                    "axis_x": "SYMBOL",
                    "axis_y": "PRICE",
                    "query": "SELECT SYMBOL, AVG(CAST(PRICE AS FLOAT)) as PRICE FROM MARKET_PRICES GROUP BY SYMBOL ORDER BY PRICE DESC"
                },
                {
                    "title": "Total Messages Ingested",
                    "type": "Card",
                    "visualKey": "total_messages",
                    "query": "SELECT COUNT(*) as COUNT FROM MARKET_PRICES"
                },
                {
                    "title": "Latest Prices",
                    "type": "Table",
                    "visualKey": "latest_prices",
                    "query": "SELECT SYMBOL, PRICE, EVENT_TIME, INGESTED_AT FROM MARKET_PRICES ORDER BY INGESTED_AT DESC LIMIT 20"
                },
                {
                    "title": "Kafka Partition Distribution",
                    "type": "PieChart",
                    "visualKey": "partition_distribution",
                    "query": "SELECT KAFKA_PARTITION, COUNT(*) as COUNT FROM MARKET_PRICES GROUP BY KAFKA_PARTITION"
                }
            ]
        }
        return json.dumps(dashboard_config, indent=2)
    
    def save_dashboard_config(self, output_path: str = "/tmp/powerbi_dashboard.json") -> None:
        """
        Save dashboard configuration to file for manual import.
        
        Args:
            output_path: Path to save the dashboard JSON configuration
        """
        config = self.create_dashboard_json()
        with open(output_path, 'w') as f:
            f.write(config)
        print(f"[{datetime.now()}] Dashboard configuration saved to {output_path}")
    
    def setup_power_bi(self) -> Dict[str, Any]:
        """
        Complete Power BI setup process.
        
        Returns:
            Setup status and configuration details
        """
        print(f"[{datetime.now()}] Starting Power BI setup...")
        print(f"[{datetime.now()}] Workspace ID: {self.workspace_id}")
        
        try:
            # Create dataset configuration
            dataset_config = self.create_market_prices_dataset()
            print(f"[{datetime.now()}] Market Prices dataset configured")
            
            # Save dashboard configuration
            self.save_dashboard_config()
            
            setup_result = {
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "workspace_id": self.workspace_id,
                "dataset": dataset_config,
                "dashboard_config_location": "/tmp/powerbi_dashboard.json",
                "instructions": {
                    "step1": "Import the saved dashboard JSON into Power BI Desktop",
                    "step2": "Connect Power BI to Snowflake using the Snowflake connector",
                    "step3": "Configure datasource with SNOWFLAKE_SERVER environment variable",
                    "step4": "Publish the report to Power BI Service",
                    "step5": "Share the dashboard with team members"
                }
            }
            
            print(f"[{datetime.now()}] Power BI setup completed successfully")
            print(json.dumps(setup_result, indent=2))
            
            return setup_result
            
        except Exception as e:
            print(f"[{datetime.now()}] Error during Power BI setup: {e}")
            raise


def main():
    """Entry point for Power BI setup."""
    try:
        manager = PowerBIManager()
        manager.setup_power_bi()
    except Exception as e:
        print(f"[{datetime.now()}] Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
