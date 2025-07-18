import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_ma_kpis(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Starting transformation of ma_logs data")
        logger.info(f"Input DataFrame columns: {df.columns.tolist()}")
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['quarter'] = df['created_at'].dt.to_period('Q').astype(str)
        result = []

        for quarter, group in df.groupby('quarter'):
            kpi = {'quarter': quarter}
            logger.info(f"Processing quarter: {quarter}")

            # Percentage of new applications evaluated on time
            new_apps = group[(group['application_type'] == 'new') & (group['event_type'] == 'evaluated') & (group['status'].notnull())]
            kpi['pct_new_apps_evaluated_on_time'] = (
                (new_apps['status'] == 'on_time').mean() * 100 if not new_apps.empty else None
            )
            logger.info(f"Quarter {quarter}: pct_new_apps_evaluated_on_time = {kpi['pct_new_apps_evaluated_on_time']}, rows = {len(new_apps)}")

            # Percentage of renewal applications evaluated on time
            renewal_apps = group[(group['application_type'] == 'renewal') & (group['event_type'] == 'evaluated') & (group['status'].notnull())]
            kpi['pct_renewal_apps_evaluated_on_time'] = (
                (renewal_apps['status'] == 'on_time').mean() * 100 if not renewal_apps.empty else None
            )
            logger.info(f"Quarter {quarter}: pct_renewal_apps_evaluated_on_time = {kpi['pct_renewal_apps_evaluated_on_time']}, rows = {len(renewal_apps)}")

            # Percentage of variation applications evaluated on time
            variation_apps = group[(group['application_type'] == 'variation') & (group['event_type'] == 'evaluated') & (group['status'].notnull())]
            kpi['pct_variation_apps_evaluated_on_time'] = (
                (variation_apps['status'] == 'on_time').mean() * 100 if not variation_apps.empty else None
            )
            logger.info(f"Quarter {quarter}: pct_variation_apps_evaluated_on_time = {kpi['pct_variation_apps_evaluated_on_time']}, rows = {len(variation_apps)}")

            # Percentage of FIR responses on time
            fir_responses = group[(group['event_type'] == 'fir_response_received') & (group['status'].notnull())]
            kpi['pct_fir_responses_on_time'] = (
                (fir_responses['status'] == 'on_time').mean() * 100 if not fir_responses.empty else None
            )
            logger.info(f"Quarter {quarter}: pct_fir_responses_on_time = {kpi['pct_fir_responses_on_time']}, rows = {len(fir_responses)}")

            # Percentage of query responses evaluated on time
            query_responses = group[(group['event_type'] == 'query_evaluated') & (group['status'].notnull())]
            kpi['pct_query_responses_evaluated_on_time'] = (
                (query_responses['status'] == 'on_time').mean() * 100 if not query_responses.empty else None
            )
            logger.info(f"Quarter {quarter}: pct_query_responses_evaluated_on_time = {kpi['pct_query_responses_evaluated_on_time']}, rows = {len(query_responses)}")

            # Percentage of applications granted within 90 days
            granted_apps = group[(group['event_type'] == 'granted') & (group['status'] == 'within_90_days') & (group['duration_days'].notnull())]
            kpi['pct_granted_within_90_days'] = (
                (granted_apps['duration_days'] <= 90).mean() * 100 if not granted_apps.empty else None
            )
            logger.info(f"Quarter {quarter}: pct_granted_within_90_days = {kpi['pct_granted_within_90_days']}, rows = {len(granted_apps)}")

            # Median duration for continental applications
            continental_apps = group[(group['is_continental'] == True) & (group['duration_days'].notnull())]
            kpi['median_duration_continental'] = (
                continental_apps['duration_days'].median() if not continental_apps.empty else None
            )
            logger.info(f"Quarter {quarter}: median_duration_continental = {kpi['median_duration_continental']}, rows = {len(continental_apps)}")

            result.append(kpi)

        kpi_df = pd.DataFrame(result)
        logger.info(f"Transformed KPI DataFrame columns: {kpi_df.columns.tolist()}")
        logger.info(f"Transformed KPI DataFrame head:\n{kpi_df.head().to_string()}")
        return kpi_df
    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}")
        raise
    