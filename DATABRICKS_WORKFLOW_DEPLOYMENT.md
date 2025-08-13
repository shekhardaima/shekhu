# Databricks Asset Bundles Workflow Deployment Guide

This guide covers deploying the data processing pipeline as a **Databricks workflow** using **Azure Databricks Asset Bundles (DABs)** for production-ready deployment across multiple environments.

## 🏗️ Architecture Overview

The pipeline is deployed as a **Databricks Job** (not notebook) with:
- **Multi-environment support** (dev/staging/prod)
- **Automated CI/CD** with GitHub Actions
- **Health monitoring** with separate monitoring job
- **Secret management** via Databricks secrets
- **Infrastructure as Code** with Asset Bundles

## 📁 Project Structure for Workflow Deployment

```
├── databricks.yml                    # Main bundle configuration
├── resources/                        # Resource definitions
│   ├── jobs.yml                     # Job definitions
│   └── secrets.yml                  # Secret scope definitions
├── src/
│   ├── workflow_main.py             # Main workflow entry point
│   ├── monitoring/
│   │   └── health_check.py          # Health check script
│   └── ... (other modules)
├── scripts/
│   └── deploy.sh                    # Deployment script
├── .github/workflows/
│   └── deploy.yml                   # CI/CD pipeline
└── init_scripts/
    └── install_dependencies.sh      # Cluster initialization
```

## 🚀 Quick Start Deployment

### 1. Prerequisites

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure

# Verify connection
databricks workspace list
```

### 2. Set Environment Variables

Create environment-specific configurations:

```bash
# For local deployment
export DATABRICKS_HOST="https://adb-xxxxx.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi-xxxxx"

# Workspace variables (set in Databricks or environment)
export workspace_id="your-workspace-id"
export workspace_region="westeurope"  # or your region
```

### 3. Deploy to Development

```bash
# Deploy to dev environment
./scripts/deploy.sh dev

# Or validate only
./scripts/deploy.sh dev --validate-only

# Force deployment
./scripts/deploy.sh dev --force
```

## 🔧 Configuration Details

### Bundle Configuration (`databricks.yml`)

The main configuration defines:
- **Multi-environment targets** (dev/staging/prod)
- **Variable overrides** per environment
- **Workspace settings** and permissions
- **Git integration** for source control

```yaml
bundle:
  name: data-processing-pipeline
  git:
    origin_url: https://github.com/your-org/data-processing-pipeline

targets:
  dev:
    default: true
    variables:
      cluster_num_workers: 2
      batch_size: 5000
  
  prod:
    variables:
      cluster_num_workers: 8
      batch_size: 20000
    run_as:
      service_principal_name: ${var.service_principal_name}
```

### Job Configuration (`resources/jobs.yml`)

Defines the main data processing job:

```yaml
resources:
  jobs:
    data_processing_pipeline:
      name: "Data Processing Pipeline - ${bundle.target}"
      
      job_clusters:
        - job_cluster_key: main_cluster
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            node_type_id: ${var.cluster_node_type}
            num_workers: ${var.cluster_num_workers}
            
      tasks:
        - task_key: data_processing_main
          python_task:
            python_file: ./src/workflow_main.py
            parameters:
              - "--batch-size"
              - ${var.batch_size}
              - "--environment"
              - ${bundle.target}
```

### Workflow Entry Point (`src/workflow_main.py`)

The main script handles:
- **Command-line arguments** parsing
- **Environment detection** and configuration
- **Secret management** from Databricks secrets
- **Error handling** and logging
- **Exit codes** for workflow status

Key features:
```python
# Environment-based secret loading
redis_config = get_databricks_secrets_config(args.environment)

# Comprehensive argument parsing
parser.add_argument("--batch-size", type=int, default=10000)
parser.add_argument("--environment", choices=["dev", "staging", "prod"])
parser.add_argument("--dry-run", action="store_true")

# Validation before execution
if not validate_configuration(redis_config):
    sys.exit(1)
```

## 🔐 Secret Management

### Setting Up Secrets

1. **Create Secret Scopes** (per environment):
```bash
# Development
databricks secrets create-scope --scope redis-secrets-dev

# Staging
databricks secrets create-scope --scope redis-secrets-staging

# Production
databricks secrets create-scope --scope redis-secrets-prod
```

2. **Add Redis Secrets**:
```bash
# For each environment
databricks secrets put --scope redis-secrets-dev --key host
databricks secrets put --scope redis-secrets-dev --key port
databricks secrets put --scope redis-secrets-dev --key password
databricks secrets put --scope redis-secrets-dev --key ssl
```

3. **Access in Code**:
```python
# Automatic environment-based secret loading
scope_name = f"redis-secrets-{environment}"
redis_config = {
    "host": dbutils.secrets.get(scope=scope_name, key="host"),
    "port": int(dbutils.secrets.get(scope=scope_name, key="port")),
    "password": dbutils.secrets.get(scope=scope_name, key="password"),
    "ssl": dbutils.secrets.get(scope=scope_name, key="ssl").lower() == "true"
}
```

## 🔄 CI/CD Pipeline

### GitHub Actions Workflow

The CI/CD pipeline includes:

1. **Testing Phase**:
   - Code linting and formatting
   - Type checking with mypy
   - Unit tests with coverage
   - Security scanning

2. **Validation Phase**:
   - Bundle validation for all environments
   - Configuration verification
   - Dependency checks

3. **Deployment Phase**:
   - **Dev**: Auto-deploy on `develop` branch
   - **Staging**: Auto-deploy on `release/*` branches
   - **Prod**: Auto-deploy on `main` branch
   - **Manual**: Via workflow_dispatch

4. **Post-Deployment**:
   - Health checks
   - Integration tests
   - Notifications

### Required GitHub Secrets

Set these in your GitHub repository settings:

```
# Databricks Authentication (per environment)
DATABRICKS_HOST_DEV=https://adb-xxxxx.azuredatabricks.net
DATABRICKS_TOKEN_DEV=dapi-xxxxx
DATABRICKS_HOST_STAGING=https://adb-xxxxx.azuredatabricks.net
DATABRICKS_TOKEN_STAGING=dapi-xxxxx
DATABRICKS_HOST_PROD=https://adb-xxxxx.azuredatabricks.net
DATABRICKS_TOKEN_PROD=dapi-xxxxx
```

## 📊 Monitoring and Health Checks

### Automated Health Monitoring

A separate monitoring job runs every 30 minutes:

```yaml
pipeline_monitoring:
  name: "Pipeline Monitoring - ${bundle.target}"
  schedule:
    quartz_cron_expression: "0 */30 * * * ?"
  tasks:
    - task_key: health_check
      python_task:
        python_file: ./src/monitoring/health_check.py
```

### Health Check Components

The health check validates:
1. **Databricks Environment** - Runtime version, Spark status
2. **Delta Table Connectivity** - Table access and statistics
3. **Redis Connectivity** - Connection and TimeSeries functionality
4. **Recent Data** - Data freshness and processing status

### Monitoring Dashboard

Access monitoring through:
- **Databricks Jobs UI** - Job run history and logs
- **Health Check Reports** - Detailed system status
- **Alerts** - Email notifications on failures
- **Metrics** - Processing statistics and performance

## 🛠️ Operations Guide

### Manual Job Execution

```bash
# Run main pipeline
databricks jobs run-now --job-name "Data Processing Pipeline - prod"

# Run health check
databricks jobs run-now --job-name "Pipeline Monitoring - prod"

# Run with custom parameters
databricks jobs run-now --job-name "Data Processing Pipeline - dev" \
  --python-params '["--batch-size", "1000", "--dry-run"]'
```

### Troubleshooting

#### Common Issues and Solutions

1. **Bundle Validation Fails**:
```bash
# Check configuration
databricks bundle validate --target dev

# Validate with verbose output
databricks bundle validate --target dev --verbose
```

2. **Job Fails to Start**:
```bash
# Check cluster configuration
databricks clusters get --cluster-id xxx

# Review job definition
databricks jobs get --job-id xxx
```

3. **Secret Access Issues**:
```bash
# List secret scopes
databricks secrets list-scopes

# Test secret access
databricks secrets get --scope redis-secrets-dev --key host
```

4. **Health Check Failures**:
```bash
# Run health check manually
python src/monitoring/health_check.py --environment dev

# Check recent job runs
databricks jobs runs list --job-id xxx --limit 10
```

### Scaling Considerations

#### Cluster Sizing by Environment

| Environment | Workers | Node Type | Batch Size | Use Case |
|-------------|---------|-----------|------------|----------|
| **Dev** | 2 | Standard_DS3_v2 | 5,000 | Development/testing |
| **Staging** | 4 | Standard_DS4_v2 | 10,000 | Integration testing |
| **Prod** | 8+ | Standard_DS5_v2 | 20,000+ | Production workload |

#### Performance Tuning

```yaml
# Cluster configuration for high-volume processing
spark_conf:
  "spark.sql.adaptive.enabled": "true"
  "spark.sql.adaptive.coalescePartitions.enabled": "true"
  "spark.databricks.delta.optimizeWrite.enabled": "true"
  "spark.databricks.delta.autoCompact.enabled": "true"
  "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
```

## 🔧 Environment-Specific Configurations

### Development Environment
- **Purpose**: Feature development and testing
- **Cluster**: Small (2 workers)
- **Schedule**: Manual/on-demand
- **Data**: Sample datasets
- **Monitoring**: Basic health checks

### Staging Environment
- **Purpose**: Integration testing and validation
- **Cluster**: Medium (4 workers)
- **Schedule**: Nightly or on release
- **Data**: Production-like datasets
- **Monitoring**: Full health checks + integration tests

### Production Environment
- **Purpose**: Live data processing
- **Cluster**: Large (8+ workers)
- **Schedule**: Daily at 2 AM UTC
- **Data**: Full production datasets
- **Monitoring**: Comprehensive monitoring + alerting

## 📚 Best Practices

### 1. **Configuration Management**
- Use environment variables for environment-specific settings
- Store sensitive data in Databricks secrets
- Version control all configuration files
- Validate configurations before deployment

### 2. **Error Handling**
- Implement comprehensive error handling
- Use appropriate exit codes for workflow status
- Log detailed error information
- Set up alerting for critical failures

### 3. **Security**
- Use service principals for production
- Implement least-privilege access
- Regularly rotate secrets and tokens
- Audit access logs

### 4. **Performance**
- Monitor job execution times
- Optimize cluster configurations
- Use Delta Lake optimizations
- Implement data caching strategies

### 5. **Maintenance**
- Regular health checks
- Monitor resource usage
- Update dependencies regularly
- Document operational procedures

## 🔗 Useful Commands

```bash
# Bundle operations
databricks bundle validate --target prod
databricks bundle deploy --target prod --force
databricks bundle destroy --target dev

# Job operations
databricks jobs list
databricks jobs get --job-id 123
databricks jobs run-now --job-name "Pipeline - prod"
databricks jobs runs list --job-id 123

# Secret operations
databricks secrets list-scopes
databricks secrets list --scope redis-secrets-prod
databricks secrets put --scope redis-secrets-prod --key host

# Workspace operations
databricks workspace list /Workspace/data-processing-pipeline
databricks workspace export /path/to/file --format SOURCE
```

## 📖 Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/)
- [Azure Databricks Best Practices](https://docs.microsoft.com/en-us/azure/databricks/)
- [Delta Lake Performance Tuning](https://docs.delta.io/latest/optimizations-oss.html)

---

This workflow deployment approach provides a production-ready, scalable solution for running your data processing pipeline in Azure Databricks with proper CI/CD, monitoring, and operational controls.