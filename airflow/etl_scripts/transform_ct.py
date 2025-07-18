import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_ct_kpis(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Starting transformation of ct_logs data")
        logger.info(f"Input DataFrame columns: {df.columns.tolist()}")
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['quarter'] = df['created_at'].dt.to_period('Q').astype(str)
        result = []

        for quarter, group in df.groupby('quarter'):
            kpi = {'quarter': quarter}
            logger.info(f"Processing quarter: {quarter}")

            # KPI 1: Percentage of new clinical trial applications evaluated on time
            new_apps_received = group[(group['application_type'] == 'new') & (group['event_type'] == 'received') & (group['status'].notnull())]
            new_apps_evaluated = group[(group['application_type'] == 'new') & (group['event_type'] == 'evaluated') & (group['status'] == 'on_time')]
            kpi['pct_new_apps_evaluated_on_time'] = (
                (len(new_apps_evaluated) / len(new_apps_received) * 100) if len(new_apps_received) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_new_apps_evaluated_on_time = {kpi['pct_new_apps_evaluated_on_time']}, received = {len(new_apps_received)}, evaluated_on_time = {len(new_apps_evaluated)}")

            # KPI 2: Percentage of amendment applications evaluated on time
            amendment_apps_received = group[(group['application_type'] == 'amendment') & (group['event_type'] == 'received') & (group['status'].notnull())]
            amendment_apps_evaluated = group[(group['application_type'] == 'amendment') & (group['event_type'] == 'evaluated') & (group['status'] == 'on_time')]
            kpi['pct_amendment_apps_evaluated_on_time'] = (
                (len(amendment_apps_evaluated) / len(amendment_apps_received) * 100) if len(amendment_apps_received) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_amendment_apps_evaluated_on_time = {kpi['pct_amendment_apps_evaluated_on_time']}, received = {len(amendment_apps_received)}, evaluated_on_time = {len(amendment_apps_evaluated)}")

            # KPI 3: Percentage of approved & ongoing clinical trials inspected per GCP plan
            gcp_inspections = group[(group['event_type'] == 'gcp_inspected') & (group['status'].notnull())]
            gcp_inspections_on_time = group[(group['event_type'] == 'gcp_inspected') & (group['status'] == 'on_time')]
            kpi['pct_gcp_inspections_on_time'] = (
                (len(gcp_inspections_on_time) / len(gcp_inspections) * 100) if len(gcp_inspections) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_gcp_inspections_on_time = {kpi['pct_gcp_inspections_on_time']}, total = {len(gcp_inspections)}, on_time = {len(gcp_inspections_on_time)}")

            # KPI 4: Percentage of safety reports assessed on time
            safety_reports_received = group[(group['event_type'] == 'safety_report_received') & (group['status'].notnull())]
            safety_reports_assessed = group[(group['event_type'] == 'safety_report_assessed') & (group['status'] == 'on_time')]
            kpi['pct_safety_reports_assessed_on_time'] = (
                (len(safety_reports_assessed) / len(safety_reports_received) * 100) if len(safety_reports_received) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_safety_reports_assessed_on_time = {kpi['pct_safety_reports_assessed_on_time']}, received = {len(safety_reports_received)}, assessed_on_time = {len(safety_reports_assessed)}")

            # KPI 5: Percentage of clinical trials compliant with GCP requirements
            gcp_inspections_compliant = group[(group['event_type'] == 'gcp_inspected') & (group['is_compliant'] == True)]
            kpi['pct_gcp_compliant'] = (
                (len(gcp_inspections_compliant) / len(gcp_inspections) * 100) if len(gcp_inspections) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_gcp_compliant = {kpi['pct_gcp_compliant']}, total = {len(gcp_inspections)}, compliant = {len(gcp_inspections_compliant)}")

            # KPI 6: Percentage of approved clinical trials listed in national registry
            approved_trials = group[(group['event_type'] == 'evaluated') & (group['status'] == 'on_time')]
            registry_submissions = group[(group['event_type'] == 'registry_submission') & (group['status'] == 'on_time')]
            kpi['pct_registry_submissions_on_time'] = (
                (len(registry_submissions) / len(approved_trials) * 100) if len(approved_trials) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_registry_submissions_on_time = {kpi['pct_registry_submissions_on_time']}, approved = {len(approved_trials)}, submitted = {len(registry_submissions)}")

            # KPI 7: Percentage of CAPA evaluated on time
            capa_received = group[(group['event_type'] == 'capa_received') & (group['status'].notnull())]
            capa_evaluated = group[(group['event_type'] == 'capa_evaluated') & (group['status'] == 'on_time')]
            kpi['pct_capa_evaluated_on_time'] = (
                (len(capa_evaluated) / len(capa_received) * 100) if len(capa_received) > 0 else None
            )
            logger.info(f"Quarter {quarter}: pct_capa_evaluated_on_time = {kpi['pct_capa_evaluated_on_time']}, received = {len(capa_received)}, evaluated_on_time = {len(capa_evaluated)}")

            # KPI 8: Average turnaround time to complete evaluation of CT applications
            evaluated_apps = group[(group['event_type'] == 'evaluated') & (group['duration_days'].notnull())]
            kpi['avg_turnaround_time'] = (
                evaluated_apps['duration_days'].mean() if not evaluated_apps.empty else None
            )
            logger.info(f"Quarter {quarter}: avg_turnaround_time = {kpi['avg_turnaround_time']}, rows = {len(evaluated_apps)}")

            result.append(kpi)

        kpi_df = pd.DataFrame(result)
        logger.info(f"Transformed KPI DataFrame columns: {kpi_df.columns.tolist()}")
        logger.info(f"Transformed KPI DataFrame head:\n{kpi_df.head().to_string()}")
        return kpi_df
    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}")
        raise