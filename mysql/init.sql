-- File: C:\Users\alber\OneDrive\Data Engineering\Projects\kpi-module\mysql\init.sql

-- Create a table for KPIs
CREATE TABLE IF NOT EXISTS kpi_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    kpi_name VARCHAR(255) NOT NULL,
    kpi_value DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample KPI data
INSERT INTO kpi_data (kpi_name, kpi_value) VALUES
    ('Revenue', 10000.50),
    ('Customer Satisfaction', 95.75);

-- Grant additional permissions to kpi_user (optional, as docker-compose already grants access)
GRANT ALL PRIVILEGES ON kpi_db.* TO 'kpi_user'@'%' IDENTIFIED BY 'kpipassword';