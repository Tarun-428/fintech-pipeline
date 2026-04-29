#!/usr/bin/env python3
"""
Power BI configuration and authentication management.
"""
import os
from typing import Optional
from msal import PublicClientApplication
from azure.identity import ClientSecretCredential


class PowerBIConfig:
    """Manages Power BI credentials and configuration."""
    
    # Azure AD Configuration
    TENANT_ID = os.getenv("POWERBI_TENANT_ID", "")
    CLIENT_ID = os.getenv("POWERBI_CLIENT_ID", "")
    CLIENT_SECRET = os.getenv("POWERBI_CLIENT_SECRET", "")
    USERNAME = os.getenv("POWERBI_USERNAME", "")
    PASSWORD = os.getenv("POWERBI_PASSWORD", "")
    
    # Power BI Configuration
    WORKSPACE_ID = os.getenv("POWERBI_WORKSPACE_ID", "")
    GROUP_ID = os.getenv("POWERBI_GROUP_ID", "")  # Alternative to WORKSPACE_ID
    
    # Snowflake Configuration for Power BI
    SNOWFLAKE_SERVER = os.getenv("SNOWFLAKE_SERVER", "")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FINTECH_ANALYTICS")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
    SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_MARKET_PRICES_TABLE", "MARKET_PRICES")
    
    # Power BI API Scopes
    SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
    
    @classmethod
    def validate_config(cls) -> None:
        """Validate that all required configuration is present."""
        required_fields = {
            "TENANT_ID": cls.TENANT_ID,
            "CLIENT_ID": cls.CLIENT_ID,
            "CLIENT_SECRET": cls.CLIENT_SECRET,
        }
        
        missing = [name for name, value in required_fields.items() if not value]
        if missing:
            raise ValueError(f"Missing required Power BI env vars: {', '.join(missing)}")
    
    @classmethod
    def get_access_token(cls) -> str:
        """
        Get an access token for Power BI API using service principal.
        
        Returns:
            Access token string
        """
        cls.validate_config()
        
        # Using MSAL for service principal authentication
        app = PublicClientApplication(
            client_id=cls.CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{cls.TENANT_ID}"
        )
        
        try:
            # For service principal, use client credentials flow
            credential = ClientSecretCredential(
                tenant_id=cls.TENANT_ID,
                client_id=cls.CLIENT_ID,
                client_secret=cls.CLIENT_SECRET
            )
            token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            return token.token
        except Exception as e:
            raise RuntimeError(f"Failed to authenticate with Power BI: {e}")
    
    @classmethod
    def get_snowflake_connection_string(cls) -> str:
        """Generate Snowflake connection string for Power BI datasource."""
        if not cls.SNOWFLAKE_SERVER:
            raise ValueError("SNOWFLAKE_SERVER environment variable not set")
        
        return f"snowflake://{cls.SNOWFLAKE_SERVER}/{cls.SNOWFLAKE_DATABASE}/{cls.SNOWFLAKE_SCHEMA}"
