from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import logging
from typing import Any, List, Dict, Optional
from datetime import datetime
from pydantic import BaseModel

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dashboard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Regulatory KPI Dashboard API",
    description="API for serving KPI data to the regulatory dashboard",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

def get_env_var(name: str, default: Any = None) -> str:
    """Get environment variable with error handling."""
    value = os.getenv(name, default)
    if value is None:
        logger.error(f"Required environment variable '{name}' not set")
        raise EnvironmentError(f"Required environment variable '{name}' not set")
    return value

# Database connection
try:
    engine = create_engine(
        f"mysql+mysqlconnector://{get_env_var('MYSQL_USER')}:{get_env_var('MYSQL_PASSWORD')}@"
        f"{get_env_var('MYSQL_HOST')}:{get_env_var('MYSQL_PORT')}/{get_env_var('MYSQL_DATABASE')}",
        pool_pre_ping=True,
        pool_recycle=3600
    )
    logger.info("Database connection established successfully")
except Exception as e:
    logger.error(f"Failed to establish database connection: {str(e)}")
    raise

class KPIResponse(BaseModel):
    id: int
    quarter: str
    year: Optional[int] = None
    created_at: Optional[str] = None
    # Add other KPI fields as needed based on your tables

def clean_dataframe(df: pd.DataFrame, float_columns: List[str]) -> pd.DataFrame:
    """Clean and prepare dataframe for JSON serialization."""
    # Convert Timestamp columns to ISO format strings
    for col in df.select_dtypes(include=['datetime64']).columns:
        df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
    
    # Clean float columns
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').replace([np.inf, -np.inf, np.nan], None)
    
    # Extract year from created_at if it exists
    if 'created_at' in df.columns:
        df['year'] = pd.to_datetime(df['created_at']).dt.year
    
    return df

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the dashboard frontend."""
    try:
        with open(os.path.join("static", "index.html")) as f:
            return f.read()
    except Exception as e:
        logger.error(f"Failed to serve index.html: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to load dashboard")

@app.get("/favicon.ico", include_in_schema=False)
async def get_favicon():
    """Serve favicon.ico."""
    favicon_path = os.path.join("static", "favicon.ico")
    if os.path.exists(favicon_path):
        return FileResponse(favicon_path)
    raise HTTPException(status_code=404, detail="Favicon not found")

@app.get("/years", response_model=List[int])
async def get_available_years():
    """Get available years with KPI data."""
    try:
        # Get years from all tables combined
        queries = [
            "SELECT DISTINCT YEAR(created_at) as year FROM kpi_ma WHERE created_at IS NOT NULL",
            "SELECT DISTINCT YEAR(created_at) as year FROM kpi_ct WHERE created_at IS NOT NULL",
            "SELECT DISTINCT YEAR(created_at) as year FROM kpi_gmp WHERE created_at IS NOT NULL"
        ]
        
        years = set()
        for query in queries:
            with engine.connect() as connection:
                result = connection.execute(text(query))
                years.update(row[0] for row in result if row[0] is not None)
        
        return sorted(years, reverse=True)
    except Exception as e:
        logger.error(f"Failed to fetch available years: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch available years")

@app.get("/ma", response_model=List[Dict[str, Any]])
async def get_ma_kpis(year: Optional[int] = None):
    """Get Marketing Authorization KPIs."""
    try:
        base_query = """
        SELECT * FROM kpi_ma 
        WHERE id IN (SELECT MAX(id) FROM kpi_ma GROUP BY quarter) 
        """
        
        if year:
            base_query += f" AND YEAR(created_at) = {year}"
            
        base_query += " ORDER BY quarter"
        
        df = pd.read_sql(base_query, engine)
        
        float_columns = [
            'pct_new_apps_evaluated_on_time',
            'pct_renewal_apps_evaluated_on_time',
            'pct_variation_apps_evaluated_on_time',
            'pct_fir_responses_on_time',
            'pct_query_responses_evaluated_on_time',
            'pct_granted_within_90_days',
            'median_duration_continental'
        ]
        
        df = clean_dataframe(df, float_columns)
        return jsonable_encoder(df.to_dict(orient='records'))
    
    except Exception as e:
        logger.error(f"Failed to fetch MA KPIs: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch MA KPIs")

@app.get("/ct", response_model=List[Dict[str, Any]])
async def get_ct_kpis(year: Optional[int] = None):
    """Get Clinical Trials KPIs."""
    try:
        base_query = """
        SELECT * FROM kpi_ct 
        WHERE id IN (SELECT MAX(id) FROM kpi_ct GROUP BY quarter) 
        """
        
        if year:
            base_query += f" AND YEAR(created_at) = {year}"
            
        base_query += " ORDER BY quarter"
        
        df = pd.read_sql(base_query, engine)
        
        float_columns = [
            'pct_new_apps_evaluated_on_time',
            'pct_amendment_apps_evaluated_on_time',
            'pct_gcp_inspections_on_time',
            'pct_safety_reports_assessed_on_time',
            'pct_gcp_compliant',
            'pct_registry_submissions_on_time',
            'pct_capa_evaluated_on_time',
            'avg_turnaround_time'
        ]
        
        df = clean_dataframe(df, float_columns)
        return jsonable_encoder(df.to_dict(orient='records'))
    
    except Exception as e:
        logger.error(f"Failed to fetch CT KPIs: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch CT KPIs")

@app.get("/gmp", response_model=List[Dict[str, Any]])
async def get_gmp_kpis(year: Optional[int] = None):
    """Get GMP Compliance KPIs."""
    try:
        base_query = """
        SELECT * FROM kpi_gmp 
        WHERE id IN (SELECT MAX(id) FROM kpi_gmp GROUP BY quarter) 
        """
        
        if year:
            base_query += f" AND YEAR(created_at) = {year}"
            
        base_query += " ORDER BY quarter"
        
        df = pd.read_sql(base_query, engine)
        
        float_columns = [
            'pct_facilities_inspected_on_time',
            'pct_complaint_inspections_on_time',
            'pct_inspections_waived_on_time',
            'pct_facilities_compliant',
            'pct_capa_decisions_on_time',
            'pct_applications_completed_on_time',
            'avg_turnaround_time',
            'median_turnaround_time',
            'pct_reports_published_on_time'
        ]
        
        df = clean_dataframe(df, float_columns)
        return jsonable_encoder(df.to_dict(orient='records'))
    
    except Exception as e:
        logger.error(f"Failed to fetch GMP KPIs: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch GMP KPIs")

# Add health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# Add CORS middleware if needed
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    