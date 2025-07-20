import random
import datetime
import mysql.connector
from faker import Faker

fake = Faker()
start_year = 2022
end_year = 2024

def random_date(year):
    month = random.randint(1, 12)
    day = random.randint(1, 28)  # safe for all months
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    return datetime.datetime(year, month, day, hour, minute)

def connect_db():
    return mysql.connector.connect(
        host='localhost',    # adjust as needed
        user='kpi_user',     # your MySQL user
        password='kpipassword',  # your MySQL password
        database='kpi_db'
    )

def generate_ma_logs(cursor, n=1000):
    app_types = ['new', 'renewal', 'variation']
    event_types = [
        'received', 'evaluated', 'granted', 
        'fir_requested', 'fir_response_received', 
        'query_received', 'query_evaluated'
    ]
    statuses = ['on_time', 'delayed', 'within_90_days', None]

    for _ in range(n):
        record = (
            fake.uuid4(),
            random.choice(app_types),
            random.choice(event_types),
            random.choice(statuses),
            random.choice([True, False]),
            random.randint(1, 180),  # duration_days
            fake.uuid4(),  # officer_id
            random_date(random.randint(start_year, end_year))
        )
        cursor.execute("""
            INSERT INTO ma_logs (
                application_id, application_type, event_type,
                status, is_continental, duration_days,
                officer_id, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, record)

def generate_ct_logs(cursor, n=1000):
    app_types = ['new', 'amendment']
    event_types = [
        'received', 'evaluated', 'gcp_inspected',
        'safety_report_received', 'safety_report_assessed',
        'registry_submission', 'capa_received', 'capa_evaluated'
    ]
    statuses = ['on_time', 'delayed', None]

    for _ in range(n):
        record = (
            fake.uuid4(),
            random.choice(app_types),
            random.choice(event_types),
            random.choice(statuses),
            random.choice([True, False]),
            random.randint(1, 120),
            fake.uuid4(),
            random_date(random.randint(start_year, end_year))
        )
        cursor.execute("""
            INSERT INTO ct_logs (
                application_id, application_type, event_type,
                status, is_compliant, duration_days,
                officer_id, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, record)

def generate_gmp_logs(cursor, n=1000):
    inspection_types = ['planned', 'complaint', 'waived']
    event_types = [
        'inspection_conducted', 'inspection_waived', 'report_published',
        'capa_received', 'capa_reviewed', 'application_received', 'application_processed'
    ]
    statuses = ['on_time', 'delayed', None]

    for _ in range(n):
        record = (
            fake.uuid4(),
            random.choice(inspection_types),
            random.choice(event_types),
            random.choice(statuses),
            random.choice([True, False]),
            random.randint(1, 150),
            fake.uuid4(),
            random_date(random.randint(start_year, end_year))
        )
        cursor.execute("""
            INSERT INTO gmp_logs (
                facility_id, inspection_type, event_type,
                status, is_compliant, duration_days,
                officer_id, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, record)

def main():
    conn = connect_db()
    cursor = conn.cursor()

    print("Generating dummy MA logs...")
    generate_ma_logs(cursor, 500)
    print("Generating dummy CT logs...")
    generate_ct_logs(cursor, 500)
    print("Generating dummy GMP logs...")
    generate_gmp_logs(cursor, 500)

    conn.commit()
    cursor.close()
    conn.close()
    print("Dummy data inserted successfully.")

if __name__ == "__main__":
    main()
