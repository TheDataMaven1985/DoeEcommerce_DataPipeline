#!/bin/bash
# Batch Job Script for DoeEcommerce Pipeline
# Location: orchestration/cron/batch_job.sh
#
# This script runs the complete data pipeline and can be scheduled via cron.
# It includes logging, error handling, and notification capabilities.
#
# Cron Schedule (Daily at 12:00 AM Lagos time):
# 0 0 * * * /path/to/batch_job.sh
#
# Usage:
#   ./batch_job.sh                    # Run full pipeline
#   ./batch_job.sh --layer bronze     # Run specific layer
#   ./batch_job.sh --dry-run          # Test without executing

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/batch_${TIMESTAMP}.log"
LOCK_FILE="/tmp/doe_pipeline.lock"
PYTHON_ENV="$PROJECT_ROOT/venv/bin/python"

# Email configuration (optional)
EMAIL_ENABLED="${EMAIL_ENABLED:-false}"
EMAIL_TO="${EMAIL_TO:-kolawolefavour20@gmail.com}"
EMAIL_FROM="${EMAIL_FROM:-noreply@company.com}"

# Pipeline configuration
PIPELINE_TIMEOUT=7200  # 2 hours in seconds
RETRY_COUNT=2
RETRY_DELAY=300  # 5 minutes

# Logging Functions
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log_error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$LOG_FILE" >&2
}

log_section() {
    echo "" | tee -a "$LOG_FILE"
    echo "============================================================" | tee -a "$LOG_FILE"
    echo "$*" | tee -a "$LOG_FILE"
    echo "============================================================" | tee -a "$LOG_FILE"
}

# Helper Functions
check_lock() {
    if [ -f "$LOCK_FILE" ]; then
        local pid=$(cat "$LOCK_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_error "Pipeline is already running (PID: $pid)"
            exit 1
        else
            log "Removing stale lock file (PID: $pid)"
            rm -f "$LOCK_FILE"
        fi
    fi
}

create_lock() {
    echo $$ > "$LOCK_FILE"
}

remove_lock() {
    rm -f "$LOCK_FILE"
}

send_email() {
    local subject="$1"
    local body="$2"
    
    if [ "$EMAIL_ENABLED" = "true" ]; then
        echo "$body" | mail -s "$subject" "$EMAIL_TO"
        log "Email notification sent: $subject"
    fi
}

check_python_env() {
    if [ ! -f "$PYTHON_ENV" ]; then
        log_error "Python environment not found: $PYTHON_ENV"
        log "Please create a virtual environment or update PYTHON_ENV variable"
        exit 1
    fi
    
    log "Using Python: $PYTHON_ENV"
}

check_database() {
    log "Checking database connection..."
    
    if ! $PYTHON_ENV -c "
import os
import sys
from pathlib import Path
sys.path.insert(0, '$PROJECT_ROOT')
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv('$PROJECT_ROOT/.env')

db_url = f\"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}\"
engine = create_engine(db_url)

try:
    with engine.connect() as conn:
        conn.execute(text('SELECT 1'))
    print('Database connection OK')
except Exception as e:
    print(f'Database connection failed: {e}')
    sys.exit(1)
" 2>&1 | tee -a "$LOG_FILE"; then
        log_error "Database connection check failed"
        return 1
    fi
    
    return 0
}

# Pipeline Execution Functions
run_pipeline() {
    local layer="${1:-all}"
    local attempt=1
    local max_attempts=$((RETRY_COUNT + 1))
    
    while [ $attempt -le $max_attempts ]; do
        log "Pipeline execution attempt $attempt/$max_attempts"
        
        if run_pipeline_once "$layer"; then
            log "Pipeline completed successfully"
            return 0
        fi
        
        if [ $attempt -lt $max_attempts ]; then
            log "Pipeline failed, retrying in $RETRY_DELAY seconds..."
            sleep $RETRY_DELAY
        fi
        
        attempt=$((attempt + 1))
    done
    
    log_error "Pipeline failed after $max_attempts attempts"
    return 1
}

run_pipeline_once() {
    local layer="$1"
    local start_time=$(date +%s)
    
    log_section "Running Pipeline - Layer: $layer"
    
    # Set timeout
    local timeout_cmd="timeout ${PIPELINE_TIMEOUT}s"
    
    # Build command
    local cmd="cd $PROJECT_ROOT && $PYTHON_ENV scripts/run_pipeline.py"
    
    if [ "$layer" != "all" ]; then
        cmd="$cmd --layer $layer"
    fi
    
    # Execute with timeout
    if $timeout_cmd bash -c "$cmd" 2>&1 | tee -a "$LOG_FILE"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        log "Pipeline completed in ${duration}s"
        return 0
    else
        local exit_code=$?
        
        if [ $exit_code -eq 124 ]; then
            log_error "Pipeline timed out after ${PIPELINE_TIMEOUT}s"
        else
            log_error "Pipeline failed with exit code: $exit_code"
        fi
        
        return 1
    fi
}

run_cleanup() {
    log_section "Running Cleanup Tasks"
    
    if cd "$PROJECT_ROOT" && $PYTHON_ENV scripts/cleanup.py --all 2>&1 | tee -a "$LOG_FILE"; then
        log "Cleanup completed successfully"
        return 0
    else
        log_error "Cleanup failed"
        return 1
    fi
}

# Monitoring and Reporting
generate_report() {
    local status="$1"
    local duration="$2"
    
    local report="
DoeEcommerce Pipeline Report
============================

Status: $status
Date: $(date +'%Y-%m-%d %H:%M:%S')
Duration: ${duration}s
Log File: $LOG_FILE

"
    
    # Add recent log entries
    report+="
Recent Log Entries:
-------------------
$(tail -n 50 "$LOG_FILE")
"
    
    echo "$report"
}

check_disk_space() {
    local min_free_space=1048576
    
    local available=$(df "$PROJECT_ROOT" | awk 'NR==2 {print $4}')
    
    if [ "$available" -lt "$min_free_space" ]; then
        log_error "Low disk space: ${available}KB available"
        return 1
    fi
    
    log "Disk space OK: ${available}KB available"
    return 0
}

# Main Execution
main() {
    local pipeline_layer="all"
    local dry_run=false
    local skip_cleanup=false
    local start_time=$(date +%s)
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --layer)
                pipeline_layer="$2"
                shift 2
                ;;
            --dry-run)
                dry_run=true
                shift
                ;;
            --skip-cleanup)
                skip_cleanup=true
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --layer LAYER       Specify pipeline layer (bronze|silver|gold|all)"
                echo "  --dry-run           Test without executing"
                echo "  --skip-cleanup      Skip cleanup tasks"
                echo "  --help              Show this help message"
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    # Create log directory
    mkdir -p "$LOG_DIR"
    
    # Start logging
    log_section "DoeEcommerce Batch Pipeline"
    log "Script: $0"
    log "Layer: $pipeline_layer"
    log "Dry Run: $dry_run"
    
    # Dry run mode
    if [ "$dry_run" = true ]; then
        log "DRY RUN MODE - No actual execution"
        log "Would execute: run_pipeline $pipeline_layer"
        exit 0
    fi
    
    # Check for existing lock
    check_lock
    create_lock
    
    # Trap to ensure lock is removed on exit
    trap remove_lock EXIT INT TERM
    
    # Pre-flight checks
    log_section "Pre-flight Checks"
    
    check_python_env || exit 1
    check_disk_space || exit 1
    check_database || exit 1
    
    # Run pipeline
    local pipeline_status="FAILED"
    
    if run_pipeline "$pipeline_layer"; then
        pipeline_status="SUCCESS"
        
        # Run cleanup if not skipped
        if [ "$skip_cleanup" = false ]; then
            run_cleanup || log "Cleanup failed but pipeline succeeded"
        fi
    fi
    
    # Calculate duration
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Generate report
    log_section "Pipeline Complete"
    log "Status: $pipeline_status"
    log "Duration: ${duration}s"
    
    # Send notification
    if [ "$pipeline_status" = "SUCCESS" ]; then
        send_email \
            " Pipeline Success - $(date +'%Y-%m-%d')" \
            "$(generate_report $pipeline_status $duration)"
        exit 0
    else
        send_email \
            " Pipeline Failed - $(date +'%Y-%m-%d')" \
            "$(generate_report $pipeline_status $duration)"
        exit 1
    fi
}

# Run main function
main "$@"
