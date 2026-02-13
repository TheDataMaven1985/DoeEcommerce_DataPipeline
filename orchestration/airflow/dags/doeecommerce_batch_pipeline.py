"""
Airflow DAG: DoeEcommerce Batch Pipeline
Location: orchestration/airflow/dags/ecommerce_batch_pipeline.py

Daily batch pipeline that runs at midnight to deliver data to 3 departments:
- Finance
- Sales
- Operations

DAG Schedule: Daily at 12:00 AM (Africa/Lagos timezone)

Pipeline Flow:
1. Ingestion Layer (Bronze) - Parallel ingestion from 3 sources
2. Transformation Layer (Silver) - Clean and deduplicate
3. Quality Checks - Validate data quality
4. KPI Publishing (Gold) - Generate department KPIs
5. Cleanup - Maintenance tasks
"""

from datetime import datetime, timedelta
from typing import Dict, Any
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email

# Import pipeline components
import sys
from pathlib import Path

# Add project root to path
project_root = Path(os.environ.get('AIRFLOW_HOME', '/opt/airflow')).parent
sys.path.insert(0, str(project_root))

from scripts.run_pipeline import RunPipeline
from scripts.cleanup import DatabaseCleaner

# DAG configuration
from airflow.models import Variable

DEFAULT_ARGS = {
    'owner': 'DoeEcommerce',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 12),
    'email': [Variable.get('pipeline_alert_email', default_var='kolawolefavour20@gmail.com')],
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Pipeline configuration
PIPELINE_CONFIG = {
    'schedule': '0 0 * * *',  # Daily at midnight
    'timezone': 'Africa/Lagos',
    'max_active_runs': 1,
    'catchup': False,
}

def send_success_notification(context: Optional[Dict[str, Any]]):
    """Send success notification. """ 
    subject = f"Pipeline success - {context['ds']}"
    body = f"""
    Pipeline completed successfully!

    Execution Date: {context['ds']}
    Duration: {context.get('dag_run').end_date - context.get('dag_run').start_date if context.get('dag_run') else 'N/A'}

    Check logs for details
    """
    alert_email = Variable.get('pipeline_alert_email', default_var='ops@company.com')
    send_email(to=alert_email, subject=subject, html_content=body) 

def send_failure_notification(context: Optional[Dict[str, Any]]):
    """Send failure notification. """
    subject = f"Pipeline failed - {context['ds']}"
    body = f"""
    Pipeline failed!

    Task: {context['task_instance'].task_id}
    Execution Date: {context['ds']}
    Error: {context.get('exception', 'Unknown error')}

    Please check the logs and fix issues
    """
    alert_email = Variable.get('pipeline_alert_email', default_var='ops@company.com')
    send_email(to=alert_email, subject=subject, html_content=body)

def send_retry_notification(context: Optional[Dict[str, Any]]):
    """Send retry notification. """
    subject = f"Retrying pipeline - {context['ds']}"
    body = f"""
    Retrying pipeline!

    Task: {context['task_instance'].task_id}
    Execution date: {context['ds']}
    """
    alert_email = Variable.get('pipeline_alert_email', default_var='ops@company.com')
    send_email(to=alert_email, subject=subject, html_content=body)

# Task functions
def run_ingestion_source(source_name: str, **context):
    """Run ingestion. """
    pipeline = RunPipeline()
    result = pipeline.run_ingestion_source(sources=[source_name])

    # Store metrics in Xcom
    context['task_instance'].xcom_push(
        key=f"{source_name}_metrics",
        value=result['sources'][0]
    )

    return result

def run_dummyjson_insgestion(**context):
    """Run ingestion from dummyjson API"""
    return run_ingestion_source('dummyjson', **context)

def run_fakestore_ingestion(**context):
    """Run ingestion from fakestore API"""
    return run_ingestion_source('fakestore', **context)

def run_randomuser_ingestion(**context):
    """Run ingestion from randomuser"""
    return run_ingestion_source('randomuser', **context)

def check_ingestion_status(**context):
    """Check if all ingestion source succeeded. """
    ti = context['task_instance']

    sources = ['dummyjson', 'fakestore', 'randomuser']
    all_success = True

    for source in sources:
        metrics = ti.xcom_pull(
            task_ids=f"ingestion.ingest_{source}",
            key=f"{source}_metrics"
        )
        if not metrics or metrics.get('status') != 'SUCCESS':
            all_success = False
            break

    return 'transformation' if all_success else 'ingestion_failed'

def run_transformation(**context):
    """Run silver layer transformation."""
    pipeline = RunPipeline()
    result = pipeline.run_transformation()
    
    context['task_instance'].xcom_push(
        key='transformation_result',
        value=result
    )
    
    return result

def run_quality_checks(**context):
    """Run data quality checks."""
    pipeline = RunPipeline()
    result = pipeline.run_quality_checks()
    
    # Check if quality checks passed
    total_failed = sum(c.get('failed', 0) for c in result.get('checks', []))
    
    context['task_instance'].xcom_push(
        key='quality_result',
        value={'total_failed': total_failed, 'details': result}
    )
    
    if total_failed > 0:
        raise ValueError(f"Quality checks failed: {total_failed} checks failed")
    
    return result

def run_gold_publishing(**context):
    """Publish KPIs to gold layer."""
    pipeline = RunPipeline()
    result = pipeline.run_gold_publishing()
    
    context['task_instance'].xcom_push(
        key='gold_result',
        value=result
    )
    
    return result

def run_cleanup_tasks(**context):
    """Run cleanup and maintenance."""
    cleaner = DatabaseCleaner()
    
    # Purge old bronze data (keep last 7 days)
    cleaner.purge_bronze_layer(days=7)
    
    # Clean audit logs (keep last 90 days)
    cleaner.clean_audit_logs(days=90)
    
    # Clean temp files
    cleaner.clean_temp_files(log_days=30)
    
    return {'status': 'SUCCESS'}

def generate_metrics_report(**context):
    """Generate and log pipeline metrics."""
    ti = context['task_instance']
    
    # Gather metrics from all stages
    ingestion_metrics = []
    for source in ['dummyjson', 'fakestore', 'randomuser']:
        metrics = ti.xcom_pull(
            task_ids=f'ingestion.ingest_{source}',
            key=f'{source}_metrics'
        )
        if metrics:
            ingestion_metrics.append(metrics)
    
    transformation_result = ti.xcom_pull(
        task_ids='transformation',
        key='transformation_result'
    )
    
    quality_result = ti.xcom_pull(
        task_ids='quality_checks',
        key='quality_result'
    )
    
    gold_result = ti.xcom_pull(
        task_ids='gold_publishing',
        key='gold_result'
    )
    
    # Log summary
    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Date: {context['ds']}")
    print(f"\nIngestion:")
    for metrics in ingestion_metrics:
        print(f"  {metrics['source']}: {metrics.get('records_loaded', 0)} records")
    
    if transformation_result:
        print(f"\nTransformation: {len(transformation_result.get('tables', []))} tables")
    
    if quality_result:
        print(f"\nQuality: {quality_result.get('total_failed', 0)} checks failed")
    
    if gold_result:
        print(f"\nGold: {len(gold_result.get('kpis', []))} KPIs published")
    
    print("=" * 60 + "\n")

# Create DAG
with DAG(
    dag_id='ecommerce_batch_pipeline',
    default_args=DEFAULT_ARGS,
    description='Daily batch pipeline for DoeEcommerce data',
    schedule_interval=PIPELINE_CONFIG['schedule'],
    max_active_runs=PIPELINE_CONFIG['max_active_runs'],
    catchup=PIPELINE_CONFIG['catchup'],
    tags=['ecommerce', 'batch', 'daily'],
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    on_retry_callback=send_retry_notification,
) as dag:
    
    # Start task
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Ingestion Layer - Parallel execution
    with TaskGroup('ingestion', tooltip='Bronze Layer - Data Ingestion') as ingestion_group:
        
        ingest_dummyjson = PythonOperator(
            task_id='ingest_dummyjson',
            python_callable=run_dummyjson_ingestion,
            provide_context=True,
        )
        
        ingest_fakestore = PythonOperator(
            task_id='ingest_fakestore',
            python_callable=run_fakestore_ingestion,
            provide_context=True,
        )
        
        ingest_randomuser = PythonOperator(
            task_id='ingest_randomuser',
            python_callable=run_randomuser_ingestion,
            provide_context=True,
        )

    # Check ingestion status
    check_ingestion = BranchPythonOperator(
        task_id='check_ingestion',
        python_callable=check_ingestion_status,
        provide_context=True,
    )
    
    # Ingestion failed handler
    ingestion_failed = BashOperator(
        task_id='ingestion_failed',
        bash_command='echo "Ingestion failed - stopping pipeline" && exit 1',
    )
    
    # Transformation Layer
    transformation = PythonOperator(
        task_id='transformation',
        python_callable=run_transformation,
        provide_context=True,
    )
    
    # Quality Checks
    quality_checks = PythonOperator(
        task_id='quality_checks',
        python_callable=run_quality_checks,
        provide_context=True,
    )
    
    # Gold Layer - KPI Publishing
    gold_publishing = PythonOperator(
        task_id='gold_publishing',
        python_callable=run_gold_publishing,
        provide_context=True,
    )
    
    # Cleanup Tasks
    cleanup = PythonOperator(
        task_id='cleanup',
        python_callable=run_cleanup_tasks,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream failed
    )
    
    # Generate Metrics Report
    metrics_report = PythonOperator(
        task_id='metrics_report',
        python_callable=generate_metrics_report,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    # End task
    end = DummyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define task dependencies
    start >> ingestion_group >> check_ingestion
    check_ingestion >> [transformation, ingestion_failed]
    transformation >> quality_checks >> gold_publishing
    gold_publishing >> cleanup >> metrics_report >> end
    ingestion_failed >> end

# DAG for manual backfills
with DAG(
    dag_id='ecommerce_backfill',
    default_args=DEFAULT_ARGS,
    description='Manual backfill for historical data',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['ecommerce', 'backfill', 'manual'],
) as backfill_dag:
    
    backfill_task = BashOperator(
        task_id='run_backfill',
        bash_command=(
            'cd {{ var.value.project_root }} && '
            'python scripts/backfill.py '
            '--days {{ dag_run.conf.get("days", 7) }} '
            '--source {{ dag_run.conf.get("source", "all") }}'
        ),
    )
