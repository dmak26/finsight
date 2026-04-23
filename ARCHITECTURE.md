# FinSight — Enterprise AI Financial Intelligence Platform

## Architecture & Technical Reference

-----

## Project Overview

FinSight is an end-to-end enterprise AI data platform combining structured market data and unstructured SEC filings with natural language querying powered by RAG (Retrieval Augmented Generation).

**Target Use Case:** A financial analyst asks a natural language question. FinSight retrieves relevant data from Snowflake, processes SEC filings through an AI pipeline, and returns an intelligent, contextual answer — all governed, secured, and auditable.

-----

## Full Technology Stack

### Source Control & CI/CD

|Tool                |Purpose                            |
|--------------------|-----------------------------------|
|GitHub              |Single source of truth for all code|
|GitHub Actions      |CI/CD pipeline automation          |
|Pre-commit hooks    |Local code quality gates           |
|Conventional commits|Standardized commit message format |

### Infrastructure as Code

|Tool           |Purpose                                        |
|---------------|-----------------------------------------------|
|Terraform      |Provisions all AWS and Snowflake infrastructure|
|Terraform Cloud|Remote state management                        |
|AWS S3 Backend |Terraform state storage                        |
|Terragrunt     |DRY Terraform patterns at scale                |

### Data Platform

|Tool                     |Purpose                            |
|-------------------------|-----------------------------------|
|Snowflake Enterprise     |Core data platform                 |
|dbt Core                 |Bronze/Silver/Gold transformations |
|Snowflake Cortex         |Native AI/ML capabilities          |
|Snowpipe                 |Automated S3-to-Snowflake ingestion|
|Snowflake Streams + Tasks|CDC and orchestration              |
|Snowflake Dynamic Tables |Declarative transformations        |
|Great Expectations       |Enterprise data quality validation |

### AWS Infrastructure

|Tool           |Purpose                           |
|---------------|----------------------------------|
|S3             |Raw file storage + Terraform state|
|IAM            |Roles and least-privilege policies|
|SNS/SQS        |Event-driven Snowpipe triggering  |
|ECS Fargate    |Containerized application hosting |
|ECR            |Container image registry          |
|Secrets Manager|Credential management             |
|CloudWatch     |Monitoring and alerting           |
|VPC            |Network security and isolation    |
|API Gateway    |REST API layer                    |

### AI/ML Layer

|Tool                    |Purpose                                 |
|------------------------|----------------------------------------|
|LangChain               |RAG orchestration framework             |
|Anthropic Claude API    |LLM reasoning engine                    |
|Snowflake Cortex Search |Vector search and embeddings            |
|MLflow                  |Experiment tracking and model versioning|
|Snowflake Model Registry|Production model management             |

### Serving Layer

|Tool      |Purpose                             |
|----------|------------------------------------|
|FastAPI   |REST API backend                    |
|Streamlit |Frontend user interface             |
|Docker    |Containerization                    |
|MCP Server|Standardized AI-to-data access layer|

### Security & Governance

|Tool                          |Purpose                         |
|------------------------------|--------------------------------|
|AWS Secrets Manager           |Production credential management|
|Snowflake RBAC                |Role-based access control       |
|Snowflake Row Access Policy   |Row-level data security         |
|Snowflake Dynamic Data Masking|PII protection                  |
|AWS IAM Roles                 |Least privilege cloud access    |
|Snowflake Audit Logging       |Compliance trail                |

### Observability

|Tool                   |Purpose                     |
|-----------------------|----------------------------|
|CloudWatch             |AWS infrastructure metrics  |
|Snowflake Query History|Query performance monitoring|
|MLflow                 |Model performance tracking  |
|dbt Tests              |Data quality monitoring     |
|Great Expectations     |Data validation checkpoints |

### Development Standards

|Tool          |Purpose                            |
|--------------|-----------------------------------|
|ruff          |Python linting and formatting      |
|pre-commit    |Automated local code checks        |
|pytest        |Unit and integration testing       |
|Docker        |Environment consistency            |
|pyproject.toml|Modern Python project configuration|

-----

## Data Sources

|Source       |Type        |Data                         |API                    |
|-------------|------------|-----------------------------|-----------------------|
|Alpha Vantage|Structured  |Stock prices, financials     |REST API (free tier)   |
|SEC EDGAR    |Unstructured|Earnings filings, 10-K, 10-Q |REST API (free, public)|
|FRED         |Structured  |Federal Reserve economic data|REST API (free)        |

-----

## End-to-End Architecture

```
Data Sources
    Alpha Vantage API
    SEC EDGAR API
    FRED API
          |
          v
Ingestion Layer
    Python Scripts
    AWS S3
    Snowpipe + SNS/SQS
          |
          v
Transformation Layer
    dbt Core
        Bronze  -- raw, minimally typed
        Silver  -- cleaned, standardized
        Gold    -- business-ready models
    Snowflake Streams + Tasks (CDC)
    Great Expectations (data quality)
          |
          v
AI/ML Layer
    Snowflake Cortex (embeddings + vector search)
    LangChain (RAG orchestration)
    Anthropic Claude API (LLM reasoning)
    MLflow (experiment tracking)
          |
          v
Serving Layer
    MCP Server  -- AI-to-Snowflake access layer
    FastAPI     -- REST API backend
    Streamlit   -- User interface
          |
          v
Infrastructure Layer
    Terraform   -- provisions everything
    Docker      -- containerizes everything
    GitHub Actions -- automates everything
          |
          v
Deployment
    AWS ECS Fargate
    Snowflake Enterprise
```

-----

## Application Architecture

```
User (Streamlit UI)
          |
          v
FastAPI REST API
          |
          v
LangChain RAG Pipeline
     |              |
     v              v
Retriever       LLM Chain
(Snowflake      (Anthropic
Cortex Search)   Claude API)
          |
          v
MCP Server
          |
          v
Snowflake Enterprise
(Gold layer + Vector Store)
```

-----

## Project Folder Structure

```
finsight/
|
|-- ingestion/
|   |-- __init__.py
|   |-- snowflake_client.py       # Reusable Snowflake connection
|   |-- alpha_vantage_loader.py   # Market data ingestion
|   |-- sec_edgar_loader.py       # SEC filings ingestion
|
|-- transformation/
|   |-- dbt_project/
|       |-- models/
|       |   |-- bronze/
|       |   |-- silver/
|       |   |-- gold/
|       |-- tests/
|       |-- macros/
|       |-- dbt_project.yml
|
|-- embeddings/
|   |-- __init__.py
|   |-- chunk_and_embed.py
|   |-- vector_store.py
|
|-- rag/
|   |-- __init__.py
|   |-- pipeline.py
|   |-- retriever.py
|   |-- prompts.py
|
|-- api/
|   |-- __init__.py
|   |-- main.py
|   |-- routes/
|   |-- schemas/
|
|-- mcp_server/
|   |-- server.py
|
|-- app/
|   |-- streamlit_app.py
|
|-- infrastructure/
|   |-- terraform/
|       |-- modules/
|       |   |-- snowflake/
|       |   |-- aws/
|       |   |-- iam/
|       |-- environments/
|       |   |-- dev/
|       |   |-- prod/
|       |-- main.tf
|       |-- variables.tf
|       |-- outputs.tf
|       |-- backend.tf
|
|-- docker/
|   |-- Dockerfile.api
|   |-- Dockerfile.app
|
|-- tests/
|   |-- unit/
|   |-- integration/
|
|-- .github/
|   |-- workflows/
|       |-- terraform.yml
|       |-- dbt.yml
|       |-- tests.yml
|
|-- .env                    # Local credentials (never committed)
|-- .env.example            # Credential template (committed)
|-- .gitignore
|-- .pre-commit-config.yaml
|-- docker-compose.yml
|-- pyproject.toml
|-- requirements.txt
|-- ARCHITECTURE.md
|-- README.md
```

-----

## CI/CD Pipeline

```
Developer pushes code
          |
          v
Pre-commit hooks (local)
    ruff lint
    ruff format
    pytest unit tests
          |
          v
GitHub Actions triggers
          |
    ______|______
   |             |
   v             v
Job 1:        Job 2:
Python Tests  Terraform
pytest        fmt check
coverage      validate
ruff          plan (PR)
              apply (main)
   |             |
   v             v
Job 3:        Job 4:
dbt           Docker
dbt test      build images
dbt run       push to ECR
Great         deploy to
Expectations  ECS Fargate
```

-----

## Snowflake Database Architecture

```
FINSIGHT (Database)
|-- RAW (Schema)
|   |-- MARKET_DATA_RAW
|   |-- SEC_FILINGS_RAW
|
|-- SILVER (Schema)
|   |-- MARKET_DATA
|   |-- SEC_FILINGS
|
|-- GOLD (Schema)
    |-- FINANCIAL_METRICS
    |-- COMPANY_PROFILES
    |-- DOCUMENT_EMBEDDINGS  -- RAG vector store
```

-----

## Security Architecture

```
Local Development
    .env file (gitignored)
    Never hardcoded in source

Production
    AWS Secrets Manager
    Injected at runtime via ECS task definition

Snowflake
    RBAC -- role per service, least privilege
    Row Access Policies -- data level security
    Dynamic Data Masking -- PII protection
    Audit logging -- compliance trail

AWS
    IAM roles -- no long-lived access keys
    VPC -- network isolation
    Security groups -- port-level control
```

-----

## Build Sequence

```
Layer 1  --  Snowflake setup + Python ingestion
Layer 2  --  dbt Bronze/Silver/Gold models
Layer 3  --  Embeddings pipeline + vector store
Layer 4  --  LangChain RAG pipeline
Layer 5  --  FastAPI REST layer
Layer 6  --  Streamlit frontend
Layer 7  --  Docker containerization
Layer 8  --  Terraform infrastructure as code
Layer 9  --  GitHub Actions CI/CD
Layer 10 --  AWS ECS Fargate deployment
```

Each layer is independently shippable.
Always working code before moving to the next layer.

-----

## What This Project Demonstrates

|Skill                             |Evidence                        |
|----------------------------------|--------------------------------|
|Full-stack data platform ownership|End-to-end from ingestion to UI |
|Infrastructure as Code            |Terraform for all resources     |
|CI/CD automation                  |GitHub Actions for all pipelines|
|Security best practices           |Secrets Manager, RBAC, IAM      |
|Data quality frameworks           |Great Expectations + dbt tests  |
|AI/ML integration                 |RAG, embeddings, LLM            |
|Containerized deployment          |Docker + ECS Fargate            |
|Financial domain expertise        |SEC filings, market data        |
|Observability and monitoring      |CloudWatch + MLflow             |
|Enterprise governance             |Snowflake RBAC, masking, audit  |

-----

## Local Setup

```bash
git clone https://github.com/dmak26/finsight.git
cd finsight
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env
# Fill in your credentials in .env
```

-----

*FinSight | Enterprise AI Financial Intelligence Platform | dmak26*