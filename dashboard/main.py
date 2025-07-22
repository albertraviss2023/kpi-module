from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, Response, RedirectResponse
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import logging
from typing import Any, List, Dict, Optional
from datetime import datetime
import re
from pydantic import BaseModel
import xlsxwriter
from io import BytesIO
import secrets

# Check for xlsxwriter availability
try:
    import xlsxwriter
    XLSXWRITER_AVAILABLE = True
except ImportError:
    XLSXWRITER_AVAILABLE = False

# Set up logging configuration with timestamp-based log file
log_filename = f'dashboard_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f"Starting new application run, logging to {log_filename}")
if not XLSXWRITER_AVAILABLE:
    logger.warning("xlsxwriter module not found; Excel export functionality will fail")

app = FastAPI(
    title="Regulatory KPI Dashboard API",
    description="API for serving KPI data to the regulatory dashboard",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Security
security = HTTPBasic()

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
logger.info("Environment variables loaded")

def get_env_var(name: str, default: Any = None) -> str:
    """Get environment variable with error handling."""
    logger.debug(f"Attempting to retrieve environment variable: {name}")
    value = os.getenv(name, default)
    if value is None:
        logger.error(f"Required environment variable '{name}' not set")
        raise EnvironmentError(f"Required environment variable '{name}' not set")
    logger.debug(f"Successfully retrieved environment variable: {name}")
    return value

# Database connection
try:
    logger.info("Attempting to establish database connection")
    engine = create_engine(
        f"mysql+mysqlconnector://{get_env_var('MYSQL_USER')}:{get_env_var('MYSQL_PASSWORD')}@"
        f"{get_env_var('MYSQL_HOST')}:{get_env_var('MYSQL_PORT')}/{get_env_var('MYSQL_DATABASE')}",
        pool_pre_ping=True,
        pool_recycle=3600
    )
    logger.info("Database connection established successfully")
except Exception as e:
    logger.error(f"Failed to establish database connection: {str(e)}", exc_info=True)
    raise

class KPIResponse(BaseModel):
    id: int
    quarter: str
    year: Optional[int] = None
    created_at: Optional[str] = None

def extract_year_from_quarter(quarter: str) -> int:
    """Extract year from quarter string (e.g., '2022Q1' -> 2022)."""
    logger.debug(f"Extracting year from quarter: {quarter}")
    if not quarter:
        logger.warning("Quarter is empty or None")
        return None
    try:
        year = int(quarter[:4])
        logger.debug(f"Extracted year: {year}")
        return year
    except (ValueError, TypeError) as e:
        logger.error(f"Failed to extract year from quarter '{quarter}': {str(e)}")
        return None

def clean_dataframe(df: pd.DataFrame, float_columns: List[str]) -> pd.DataFrame:
    """Clean and prepare dataframe for JSON serialization."""
    logger.info("Starting dataframe cleaning process")
    try:
        # Convert Timestamp columns to ISO format strings
        for col in df.select_dtypes(include=['datetime64']).columns:
            logger.debug(f"Converting datetime column: {col}")
            df[col] = df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        
        # Clean float columns
        for col in float_columns:
            if col in df.columns:
                logger.debug(f"Cleaning float column: {col}")
                df[col] = pd.to_numeric(df[col], errors='coerce').replace([np.inf, -np.inf, np.nan], None)
        
        # Extract year from quarter if it exists
        if 'quarter' in df.columns:
            logger.debug("Extracting year from quarter column")
            df['year'] = df['quarter'].apply(extract_year_from_quarter)
        
        logger.info("Dataframe cleaning completed successfully")
        return df
    except Exception as e:
        logger.error(f"Failed to clean dataframe: {str(e)}", exc_info=True)
        raise

# Authentication function
def authenticate_user(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, "admin")
    correct_password = secrets.compare_digest(credentials.password, get_env_var("ADMIN_PASSWORD"))
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the login page."""
    logger.info("Serving root endpoint (login page)")
    try:
        with open(os.path.join("static", "index.html")) as f:
            content = f.read()
            logger.debug("Successfully read index.html")
            return content
    except Exception as e:
        logger.error(f"Failed to serve index.html: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to load login page")

@app.get("/dashboard", response_class=HTMLResponse)
async def read_dashboard():
    """Serve the dashboard page."""
    logger.info("Serving dashboard endpoint")
    try:
        with open(os.path.join("static", "dashboard.html")) as f:
            content = f.read()
            logger.debug("Successfully read dashboard.html")
            return content
    except Exception as e:
        logger.error(f"Failed to serve dashboard.html: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to load dashboard")

@app.get("/favicon.ico", include_in_schema=False)
async def get_favicon():
    """Serve favicon.ico."""
    logger.info("Serving favicon.ico")
    favicon_path = os.path.join("static", "favicon.ico")
    if os.path.exists(favicon_path):
        logger.debug("Favicon found, serving file")
        return FileResponse(favicon_path)
    logger.warning("Favicon not found")
    raise HTTPException(status_code=404, detail="Favicon not found")

@app.get("/years", response_model=List[int])
async def get_available_years():
    """Get available years with KPI data."""
    logger.info("Fetching available years")
    try:
        queries = [
            "SELECT DISTINCT quarter FROM kpi_ma WHERE quarter IS NOT NULL",
            "SELECT DISTINCT quarter FROM kpi_ct WHERE quarter IS NOT NULL",
            "SELECT DISTINCT quarter FROM kpi_gmp WHERE quarter IS NOT NULL"
        ]
        
        years = set()
        for query in queries:
            logger.debug(f"Executing query: {query}")
            with engine.connect() as connection:
                result = connection.execute(text(query))
                for row in result:
                    if row[0]:
                        year = extract_year_from_quarter(row[0])
                        if year:
                            years.add(year)
        
        result = sorted(years, reverse=True)
        logger.info(f"Retrieved {len(result)} unique years")
        return result
    except Exception as e:
        logger.error(f"Failed to fetch available years: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch available years")

@app.get("/ma", response_model=List[Dict[str, Any]])
async def get_ma_kpis(year: Optional[int] = None):
    """Get Marketing Authorization KPIs."""
    logger.info(f"Fetching MA KPIs for year: {year if year else 'all years'}")
    try:
        base_query = """
        SELECT * FROM kpi_ma 
        WHERE id IN (SELECT MAX(id) FROM kpi_ma GROUP BY quarter) 
        """
        
        if year:
            base_query += f" AND quarter LIKE '{year}%'"
            
        base_query += " ORDER BY quarter"
        logger.debug(f"Executing MA query: {base_query}")
        
        df = pd.read_sql(base_query, engine)
        logger.debug(f"Retrieved {len(df)} MA KPI records")
        
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
        result = jsonable_encoder(df.to_dict(orient='records'))
        logger.info("Successfully processed MA KPIs")
        return result
    
    except Exception as e:
        logger.error(f"Failed to fetch MA KPIs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch MA KPIs")

@app.get("/ct", response_model=List[Dict[str, Any]])
async def get_ct_kpis(year: Optional[int] = None):
    """Get Clinical Trials KPIs."""
    logger.info(f"Fetching CT KPIs for year: {year if year else 'all years'}")
    try:
        base_query = """
        SELECT * FROM kpi_ct 
        WHERE id IN (SELECT MAX(id) FROM kpi_ct GROUP BY quarter) 
        """
        
        if year:
            base_query += f" AND quarter LIKE '{year}%'"
            
        base_query += " ORDER BY quarter"
        logger.debug(f"Executing CT query: {base_query}")
        
        df = pd.read_sql(base_query, engine)
        logger.debug(f"Retrieved {len(df)} CT KPI records")
        
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
        result = jsonable_encoder(df.to_dict(orient='records'))
        logger.info("Successfully processed CT KPIs")
        return result
    
    except Exception as e:
        logger.error(f"Failed to fetch CT KPIs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch CT KPIs")

@app.get("/gmp", response_model=List[Dict[str, Any]])
async def get_gmp_kpis(year: Optional[int] = None):
    """Get GMP Compliance KPIs."""
    logger.info(f"Fetching GMP KPIs for year: {year if year else 'all years'}")
    try:
        base_query = """
        SELECT * FROM kpi_gmp 
        WHERE id IN (SELECT MAX(id) FROM kpi_gmp GROUP BY quarter) 
        """
        
        if year:
            base_query += f" AND quarter LIKE '{year}%'"
            
        base_query += " ORDER BY quarter"
        logger.debug(f"Executing GMP query: {base_query}")
        
        df = pd.read_sql(base_query, engine)
        logger.debug(f"Retrieved {len(df)} GMP KPI records")
        
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
        result = jsonable_encoder(df.to_dict(orient='records'))
        logger.info("Successfully processed GMP KPIs")
        return result
    
    except Exception as e:
        logger.error(f"Failed to fetch GMP KPIs: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch GMP KPIs")

@app.get("/export/all")
async def export_all_data():
    """Export all KPI data to Excel."""
    logger.info("Exporting all KPI data to Excel")
    if not XLSXWRITER_AVAILABLE:
        logger.error("Cannot export to Excel: xlsxwriter module not installed")
        raise HTTPException(status_code=500, detail="Excel export unavailable: xlsxwriter module not installed")
    try:
        logger.debug("Fetching data from kpi_ma table")
        ma_df = pd.read_sql("SELECT * FROM kpi_ma", engine)
        logger.debug(f"Retrieved {len(ma_df)} MA records")
        
        logger.debug("Fetching data from kpi_ct table")
        ct_df = pd.read_sql("SELECT * FROM kpi_ct", engine)
        logger.debug(f"Retrieved {len(ct_df)} CT records")
        
        logger.debug("Fetching data from kpi_gmp table")
        gmp_df = pd.read_sql("SELECT * FROM kpi_gmp", engine)
        logger.debug(f"Retrieved {len(gmp_df)} GMP records")
        
        # Add year column based on quarter
        for df in [ma_df, ct_df, gmp_df]:
            if 'quarter' in df.columns:
                logger.debug("Adding year column to dataframe")
                df['year'] = df['quarter'].apply(extract_year_from_quarter)
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            logger.debug("Writing MA data to Excel")
            ma_df.to_excel(writer, sheet_name='Marketing Authorization', index=False)
            logger.debug("Writing CT data to Excel")
            ct_df.to_excel(writer, sheet_name='Clinical Trials', index=False)
            logger.debug("Writing GMP data to Excel")
            gmp_df.to_excel(writer, sheet_name='GMP Compliance', index=False)
        
        output.seek(0)
        
        headers = {
            'Content-Disposition': 'attachment; filename="all_kpi_data.xlsx"',
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        
        logger.info("Successfully generated Excel file for all KPI data")
        return Response(content=output.read(), headers=headers)
    
    except Exception as e:
        logger.error(f"Failed to export data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export data")

@app.get("/export/by-year")
async def export_data_by_years(
    years: str = Query(..., description="Comma-separated list of years to export"),
    processes: str = Query("ma,ct,gmp", description="Comma-separated list of process types to include")
):
    """Export KPI data for specific years."""
    logger.info(f"Exporting KPI data for years: {years}, processes: {processes}")
    if not XLSXWRITER_AVAILABLE:
        logger.error("Cannot export to Excel: xlsxwriter module not installed")
        raise HTTPException(status_code=500, detail="Excel export unavailable: xlsxwriter module not installed")
    try:
        year_list = [int(y) for y in years.split(',')]
        process_list = processes.split(',')
        logger.debug(f"Parsed years: {year_list}, processes: {process_list}")
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if 'ma' in process_list:
                ma_query = "SELECT * FROM kpi_ma WHERE quarter LIKE :year_pattern"
                for year in year_list:
                    logger.debug(f"Fetching MA data for year: {year}")
                    ma_df = pd.read_sql(
                        text(ma_query), 
                        engine, 
                        params={'year_pattern': f'{year}%'}
                    )
                    if not ma_df.empty:
                        logger.debug(f"Writing MA data for year {year} to Excel")
                        ma_df.to_excel(writer, sheet_name=f'MA {year}', index=False)
            
            if 'ct' in process_list:
                ct_query = "SELECT * FROM kpi_ct WHERE quarter LIKE :year_pattern"
                for year in year_list:
                    logger.debug(f"Fetching CT data for year: {year}")
                    ct_df = pd.read_sql(
                        text(ct_query), 
                        engine, 
                        params={'year_pattern': f'{year}%'}
                    )
                    if not ct_df.empty:
                        logger.debug(f"Writing CT data for year {year} to Excel")
                        ct_df.to_excel(writer, sheet_name=f'CT {year}', index=False)
            
            if 'gmp' in process_list:
                gmp_query = "SELECT * FROM kpi_gmp WHERE quarter LIKE :year_pattern"
                for year in year_list:
                    logger.debug(f"Fetching GMP data for year: {year}")
                    gmp_df = pd.read_sql(
                        text(gmp_query), 
                        engine, 
                        params={'year_pattern': f'{year}%'}
                    )
                    if not gmp_df.empty:
                        logger.debug(f"Writing GMP data for year {year} to Excel")
                        gmp_df.to_excel(writer, sheet_name=f'GMP {year}', index=False)
        
        output.seek(0)
        
        headers = {
            'Content-Disposition': f'attachment; filename="kpi_data_{years.replace(",", "_")}.xlsx"',
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        
        logger.info("Successfully generated Excel file for selected years")
        return Response(content=output.read(), headers=headers)
    
    except Exception as e:
        logger.error(f"Failed to export data by year: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to export data by year")

# Add health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    logger.info("Health check requested")
    try:
        status = {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
        logger.debug(f"Health check response: {status}")
        return status
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Health check failed")

# Add CORS middleware if needed
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.info("CORS middleware configured")

if __name__ == "__main__":
    logger.info("Starting FastAPI application")
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    