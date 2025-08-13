#!/bin/bash
set -e

# Data Processing Pipeline - Databricks Asset Bundle Deployment Script
# Usage: ./scripts/deploy.sh [environment] [options]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
ENVIRONMENT="dev"
VALIDATE_ONLY=false
FORCE_DEPLOY=false
DRY_RUN=false
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
Data Processing Pipeline - Databricks Deployment Script

Usage: $0 [ENVIRONMENT] [OPTIONS]

ENVIRONMENTS:
    dev         Deploy to development environment (default)
    staging     Deploy to staging environment
    prod        Deploy to production environment

OPTIONS:
    --validate-only     Only validate the bundle without deploying
    --force            Force deployment even if validation warnings exist
    --dry-run          Show what would be deployed without actually deploying
    --verbose          Enable verbose logging
    --help             Show this help message

EXAMPLES:
    $0 dev                          # Deploy to development
    $0 prod --validate-only         # Validate production bundle
    $0 staging --force              # Force deploy to staging
    $0 dev --dry-run --verbose      # Dry run with verbose output

PREREQUISITES:
    - Databricks CLI installed and configured
    - Valid Databricks workspace connection
    - Appropriate permissions for target environment
    - Environment variables set (see README for details)

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        dev|staging|prod)
            ENVIRONMENT="$1"
            shift
            ;;
        --validate-only)
            VALIDATE_ONLY=true
            shift
            ;;
        --force)
            FORCE_DEPLOY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Set verbose mode if requested
if [ "$VERBOSE" = true ]; then
    set -x
fi

log_info "Starting deployment for environment: $ENVIRONMENT"

# Change to project root directory
cd "$PROJECT_ROOT"

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Databricks CLI is installed
    if ! command -v databricks &> /dev/null; then
        log_error "Databricks CLI is not installed. Please install it first."
        log_info "Install with: pip install databricks-cli"
        exit 1
    fi
    
    # Check if we can authenticate with Databricks
    if ! databricks auth profiles &> /dev/null; then
        log_error "Databricks authentication not configured."
        log_info "Run 'databricks configure' to set up authentication."
        exit 1
    fi
    
    # Check if bundle configuration exists
    if [ ! -f "databricks.yml" ]; then
        log_error "databricks.yml not found in project root."
        exit 1
    fi
    
    # Check if source code exists
    if [ ! -d "src" ]; then
        log_error "src directory not found."
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Validate bundle configuration
validate_bundle() {
    log_info "Validating bundle configuration for $ENVIRONMENT..."
    
    if databricks bundle validate --target "$ENVIRONMENT"; then
        log_success "Bundle validation passed"
        return 0
    else
        log_error "Bundle validation failed"
        return 1
    fi
}

# Deploy bundle
deploy_bundle() {
    log_info "Deploying bundle to $ENVIRONMENT..."
    
    local deploy_cmd="databricks bundle deploy --target $ENVIRONMENT"
    
    if [ "$FORCE_DEPLOY" = true ]; then
        deploy_cmd="$deploy_cmd --force"
        log_warning "Force deployment enabled - this will overwrite existing resources"
    fi
    
    if [ "$DRY_RUN" = true ]; then
        log_info "DRY RUN: Would execute: $deploy_cmd"
        return 0
    fi
    
    if eval "$deploy_cmd"; then
        log_success "Bundle deployed successfully"
        return 0
    else
        log_error "Bundle deployment failed"
        return 1
    fi
}

# Set up secrets (if needed)
setup_secrets() {
    log_info "Setting up secrets for $ENVIRONMENT..."
    
    local scope_name="redis-secrets-$ENVIRONMENT"
    
    # Check if secrets scope exists
    if databricks secrets list-scopes | grep -q "$scope_name"; then
        log_info "Secrets scope '$scope_name' already exists"
    else
        log_warning "Secrets scope '$scope_name' does not exist"
        log_info "You may need to create it manually and add the required secrets:"
        log_info "  databricks secrets create-scope --scope $scope_name"
        log_info "  databricks secrets put --scope $scope_name --key host"
        log_info "  databricks secrets put --scope $scope_name --key port"
        log_info "  databricks secrets put --scope $scope_name --key password"
        log_info "  databricks secrets put --scope $scope_name --key ssl"
    fi
}

# Run post-deployment tests
run_post_deployment_tests() {
    log_info "Running post-deployment tests..."
    
    # Test if the job was created successfully
    local job_name="Data Processing Pipeline - $ENVIRONMENT"
    
    if databricks jobs list | grep -q "$job_name"; then
        log_success "Job '$job_name' created successfully"
    else
        log_warning "Job '$job_name' not found in job list"
    fi
    
    # Test health check job
    local health_job_name="Pipeline Monitoring - $ENVIRONMENT"
    
    if databricks jobs list | grep -q "$health_job_name"; then
        log_success "Health check job '$health_job_name' created successfully"
    else
        log_warning "Health check job '$health_job_name' not found"
    fi
}

# Main deployment flow
main() {
    log_info "Data Processing Pipeline Deployment"
    log_info "Environment: $ENVIRONMENT"
    log_info "Validate only: $VALIDATE_ONLY"
    log_info "Force deploy: $FORCE_DEPLOY"
    log_info "Dry run: $DRY_RUN"
    
    # Check prerequisites
    check_prerequisites
    
    # Validate bundle
    if ! validate_bundle; then
        if [ "$FORCE_DEPLOY" = true ]; then
            log_warning "Validation failed but continuing due to --force flag"
        else
            log_error "Validation failed. Use --force to deploy anyway."
            exit 1
        fi
    fi
    
    # If validate-only mode, exit here
    if [ "$VALIDATE_ONLY" = true ]; then
        log_success "Validation completed successfully"
        exit 0
    fi
    
    # Set up secrets
    setup_secrets
    
    # Deploy bundle
    if ! deploy_bundle; then
        log_error "Deployment failed"
        exit 1
    fi
    
    # Run post-deployment tests
    run_post_deployment_tests
    
    log_success "Deployment completed successfully!"
    
    # Show useful information
    log_info "Next steps:"
    log_info "1. Check the deployed jobs in Databricks workspace"
    log_info "2. Configure job schedules if needed"
    log_info "3. Set up monitoring and alerting"
    log_info "4. Run a test job to verify functionality"
    
    # Show job run command
    log_info ""
    log_info "To run the pipeline job manually:"
    log_info "  databricks jobs run-now --job-name 'Data Processing Pipeline - $ENVIRONMENT'"
    
    log_info ""
    log_info "To check health:"
    log_info "  databricks jobs run-now --job-name 'Pipeline Monitoring - $ENVIRONMENT'"
}

# Run main function
main "$@"