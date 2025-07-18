import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_gmp_kpis(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Starting transformation of gmp_logs data")
        logger.info(f"Input DataFrame columns: {df.columns.tolist()}")
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['quarter'] = df['created_at'].dt.to_period('Q').astype(str)
        result = []

        # Log raw data counts by event_type, inspection_type, and status
        logger.info("Raw data distribution:")
        raw_counts = df.groupby(['quarter', 'event_type', 'inspection_type', 'status', 'is_compliant']).size().reset_index(name='count')
        logger.info(f"\n{raw_counts.to_string()}")

        for quarter, group in df.groupby('quarter'):
            kpi = {'quarter': quarter}
            logger.info(f"Processing quarter: {quarter}")

            # KPI 1: Percentage of facilities inspected for GMP as per plan
            inspections_planned = group[(group['event_type'] == 'inspection_conducted') & 
                                       (group['inspection_type'] == 'planned')]
            inspections_on_time = group[(group['event_type'] == 'inspection_conducted') & 
                                       (group['inspection_type'] == 'planned') & 
                                       (group['status'] == 'on_time')]
            kpi['pct_facilities_inspected_on_time'] = (
                (len(inspections_on_time) / len(inspections_planned) * 100) if len(inspections_planned) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_facilities_inspected_on_time = {kpi['pct_facilities_inspected_on_time']}, planned = {len(inspections_planned)}, on_time = {len(inspections_on_time)}")

            # KPI 2: Percentage of complaint-triggered GMP inspections conducted
            complaint_inspections = group[(group['event_type'] == 'inspection_conducted') & 
                                         (group['inspection_type'] == 'complaint')]
            complaint_inspections_on_time = group[(group['event_type'] == 'inspection_conducted') & 
                                                (group['inspection_type'] == 'complaint') & 
                                                (group['status'] == 'on_time')]
            kpi['pct_complaint_inspections_on_time'] = (
                (len(complaint_inspections_on_time) / len(complaint_inspections) * 100) if len(complaint_inspections) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_complaint_inspections_on_time = {kpi['pct_complaint_inspections_on_time']}, total = {len(complaint_inspections)}, on_time = {len(complaint_inspections_on_time)}")

            # KPI 3: Percentage of GMP inspections waived within set timelines
            all_inspections = group[group['event_type'].isin(['inspection_conducted', 'inspection_waived'])]
            inspections_waived_on_time = group[(group['event_type'] == 'inspection_waived') & 
                                              (group['status'] == 'on_time')]
            kpi['pct_inspections_waived_on_time'] = (
                (len(inspections_waived_on_time) / len(all_inspections) * 100) if len(all_inspections) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_inspections_waived_on_time = {kpi['pct_inspections_waived_on_time']}, total = {len(all_inspections)}, waived_on_time = {len(inspections_waived_on_time)}")

            # KPI 4: Percentage of facilities compliant with GMP requirements
            inspections_compliant = group[(group['event_type'].isin(['inspection_conducted', 'inspection_waived'])) & 
                                         (group['is_compliant'] == True)]
            kpi['pct_facilities_compliant'] = (
                (len(inspections_compliant) / len(all_inspections) * 100) if len(all_inspections) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_facilities_compliant = {kpi['pct_facilities_compliant']}, total = {len(all_inspections)}, compliant = {len(inspections_compliant)}")

            # KPI 5: Percentage of final CAPA decisions issued within specified timeline
            capa_received = group[(group['event_type'] == 'capa_received')]
            capa_decisions_on_time = group[(group['event_type'] == 'capa_reviewed') & 
                                          (group['status'] == 'on_time')]
            kpi['pct_capa_decisions_on_time'] = (
                (len(capa_decisions_on_time) / len(capa_received) * 100) if len(capa_received) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_capa_decisions_on_time = {kpi['pct_capa_decisions_on_time']}, received = {len(capa_received)}, on_time = {len(capa_decisions_on_time)}")

            # KPI 6: Percentage of GMP inspection applications completed within set timeline
            applications_received = group[(group['event_type'] == 'application_received')]
            applications_completed_on_time = group[(group['event_type'] == 'application_processed') & 
                                                 (group['status'] == 'on_time')]
            kpi['pct_applications_completed_on_time'] = (
                (len(applications_completed_on_time) / len(applications_received) * 100) if len(applications_received) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_applications_completed_on_time = {kpi['pct_applications_completed_on_time']}, received = {len(applications_received)}, completed_on_time = {len(applications_completed_on_time)}")

            # KPI 7: Average turnaround time to complete GMP applications
            applications_processed = group[(group['event_type'] == 'application_processed') & 
                                         (group['duration_days'].notnull())]
            kpi['avg_turnaround_time'] = (
                applications_processed['duration_days'].mean() if not applications_processed.empty else None
            )
            logger.info(f"Quarter {quarter}: avg_turnaround_time = {kpi['avg_turnaround_time']}, rows = {len(applications_processed)}")

            # KPI 8: Median turnaround time to complete GMP inspection applications
            kpi['median_turnaround_time'] = (
                applications_processed['duration_days'].median() if not applications_processed.empty else None
            )
            logger.info(f"Quarter {quarter}: median_turnaround_time = {kpi['median_turnaround_time']}, rows = {len(applications_processed)}")

            # KPI 9: Percentage of GMP inspection reports published within specified timeline
            reports_published_on_time = group[(group['event_type'] == 'report_published') & 
                                            (group['status'] == 'on_time')]
            kpi['pct_reports_published_on_time'] = (
                (len(reports_published_on_time) / len(all_inspections) * 100) if len(all_inspections) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_reports_published_on_time = {kpi['pct_reports_published_on_time']}, total_inspections = {len(all_inspections)}, published_on_time = {len(reports_published_on_time)}")

            result.append(kpi)

        kpi_df = pd.DataFrame(result)
        logger.info(f"Transformed KPI DataFrame columns: {kpi_df.columns.tolist()}")
        logger.info(f"Transformed KPI DataFrame head:\n{kpi_df.head().to_string()}")
        return kpi_df
    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}")
        raise
    