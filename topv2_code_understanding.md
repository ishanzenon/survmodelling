# TOP v2 Codebase - Code Understanding Guide

## Overview

The **TOP v2 (Turnover Probability v2)** codebase is a complete rewrite of ADP's employee turnover prediction ETL pipeline. This new implementation consolidates training and prediction data preparation into a unified pipeline using survival analysis methodology.

## Project Context

- **Client**: ADP (major HR services company)
- **Goal**: Predict employee turnover probability using survival analysis
- **Timeline**: 4-week production deployment
- **Users**: 30,000+ monthly HR practitioners
- **Methodology**: Transition from XGBoost classification to Cox PH + XGBoost AFT survival models

## Architecture Overview

The codebase follows a modular ETL design with clean separation of concerns:

```
top_etl/
├── main.py              # Main orchestration pipeline
├── tables.py            # Core table loading and transformation logic
├── utils.py             # Utility functions for analysis and display
└── transforms/
    ├── clean.py         # Data cleaning transformations
    └── normalize.py     # Data normalization transformations
```

## Key Components

### 1. Main Pipeline (`main.py`)
**Purpose**: Orchestrates the entire ETL process

**Key Flow**:
1. Load dimension tables (client, person, job, profile)
2. Load fact tables (work assignments, work events)
3. Load hierarchy data (manager/supervisor relationships)
4. Transform to weekly episodes format
5. Apply business rules and normalizations
6. Calculate team-level aggregations
7. Output final survival analysis dataset

### 2. Table Operations (`tables.py`)
**Purpose**: Contains all table loading and transformation functions

**Key Functions**:
- `load_*()` functions: Load and clean dimension/fact tables
- `calc_*()` functions: Perform complex transformations and joins
- Handles data quality issues and business logic
- Manages weekly episode conversion for survival analysis

### 3. Data Transformations (`transforms/`)

#### Clean Transformations (`clean.py`)
- String cleaning and normalization
- Date/timestamp conversions
- Source system filtering
- Live client filtering

#### Normalize Transformations (`normalize.py`)
- Weekly episode generation (start-stop format)
- Forward-fill missing job codes
- Manager/supervisor ID normalization
- Employment classification standardization
- Termination type classification

### 4. Utilities (`utils.py`)
**Purpose**: Analysis and debugging utilities

**Key Features**:
- Spark to Polars conversion for better display
- Data quality assessment functions
- Nullity analysis and visualization
- Quick data exploration functions

## Data Flow

```
Source Tables → Load & Clean → Transform → Normalize → Weekly Episodes → Final Dataset
```

1. **Source Systems**: ADPA warehouse, benchmarks tables
2. **Dimension Tables**: Client, person, job, profile attributes
3. **Fact Tables**: Work assignments, work events, hierarchy
4. **Transformations**: Clean, normalize, standardize
5. **Output**: Weekly start-stop format for survival analysis

## Key Data Concepts

### Weekly Episodes
- Converts employment spans into weekly intervals
- Uses ISO week format (YYYY-WXX)
- Handles time-varying covariates efficiently
- Supports survival analysis methodology

### Start-Stop Format
Organizes data by periods where no time-varying covariates change:
```
pers_id | start_week | end_week | salary | job_cd | term_type
   1    | 2020-W01  | 2021-W32 | 50000  |  123   |   NULL
   1    | 2021-W33  | 2022-W50 | 55000  |  123   |   NULL
   1    | 2022-W50  | 2022-W51 | 55000  |  123   |   Vol
```

### Business Rules
- Termination types: Voluntary (Vol), Involuntary (Inv), Unknown (Unk)
- Employment status: Full-time/Part-time, Regular/Temporary
- Manager hierarchy: Validated span of control, invalid managers filtered
- Data quality: Known issues handled (default managers, missing commute data)

## Input/Output Tables

### Key Input Tables
- **Client Master**: `client_master` (NAICS codes, opt-out filtering)
- **Person Dimension**: `dwh_t_dim_pers` (demographics)
- **Work Assignments**: `dwh_t_fact_work_asgmt` (main fact table)
- **Work Events**: `dwh_t_fact_work_event` (termination events)
- **Hierarchy**: `dwh_t_mngr_hrchy_sec`, `dwh_t_supvr_hrchy_sec`

### Output Table
- **Final Dataset**: `zenon_topv2_2020_2024`
- Format: Weekly start-stop episodes
- Contains: All features needed for survival analysis modeling

## Development Guidelines

### Code Style
- Uses PySpark DataFrame operations
- Leverages `@spark_transform` decorator for function chaining
- Consistent naming conventions (snake_case)
- Comprehensive docstrings with business context

### Data Quality
- Handles known data quality issues explicitly
- Validates transformations with built-in checks
- Provides nullity analysis and data profiling utilities
- Includes edge case handling for termination records

### Performance
- Optimized for large-scale data processing
- Configurable Spark partitioning
- Efficient join strategies
- Minimal data shuffling

## Quick Start

1. **Explore the pipeline**: Start with `main.py` to understand the flow
2. **Understand transformations**: Review `tables.py` for business logic
3. **Debug data**: Use utilities in `utils.py` for data exploration
4. **Modify transforms**: Edit functions in `transforms/` for business rule changes

## Common Tasks

- **Adding new features**: Modify relevant `calc_*()` functions in `tables.py`
- **Changing business rules**: Update transformation functions in `transforms/`
- **Data quality analysis**: Use `quick_view()`, `get_nullity_stats()` from `utils.py`
- **Performance tuning**: Adjust Spark configuration in `main.py`

## Testing and Validation

The codebase includes built-in data validation and quality checks:
- Automatic nullity analysis
- Data type validation
- Business rule verification
- Performance monitoring utilities

---

*For detailed technical specifications, refer to the individual module docstrings and the comprehensive project documentation.*