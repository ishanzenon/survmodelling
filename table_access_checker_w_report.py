"""
ADP Turnover Probability - Enhanced Table Access Report Generator (Fixed)

CRITICAL FIXES IMPLEMENTED:
1. Consistent DataFrame pattern (removed cursor/tuple inconsistency)
2. Proper Databricks exception handling
3. Batch metadata queries for performance
4. Progress tracking for long operations

SETUP INSTRUCTIONS:
1. Update the three connection variables below
2. Ensure your Databricks notebook has PySpark access
3. Run the script to generate comprehensive table reports
"""

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pandas as pd
import re
import logging
from datetime import datetime
from collections import defaultdict
import time

# =============================================================================
# CONNECTION CONFIGURATION - UPDATE THESE VALUES
# =============================================================================
SERVER_HOSTNAME = "adpdc-share1-dev.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/6a8db13cc09aab90"
ACCESS_TOKEN = "dapi5efcb10de3d52a4f0774b7ade88ed80b"

# Unity Catalog configuration
CATALOG_NAME = "onedata_us_east_1_shared_dit"

# =============================================================================
# SCRIPT CONFIGURATION
# =============================================================================

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print(f"🎯 ADP Turnover Probability - Enhanced Table Access Report Generator (Fixed)")
print(f"Using PySpark with Databricks: {SERVER_HOSTNAME}")
print(f"Unity Catalog: {CATALOG_NAME}")
print("=" * 80)

# =============================================================================
# FIXED PYSPARK CONNECTION - CONSISTENT DATAFRAME PATTERN
# =============================================================================

class DataFrameConnection:
    """
    Fixed connection class using consistent DataFrame pattern
    No more cursor/tuple inconsistency
    """
    def __init__(self, spark_session):
        self.spark = spark_session
        print(f"✅ Connected to PySpark session: {spark_session.version}")
    
    def sql(self, query):
        """Execute SQL and return DataFrame directly"""
        try:
            return self.spark.sql(query)
        except Exception as e:
            raise e
    
    def sql_to_pandas(self, query):
        """Execute SQL and return pandas DataFrame"""
        try:
            spark_df = self.spark.sql(query)
            return spark_df.toPandas()
        except Exception as e:
            raise e
    
    def close(self):
        """Close connection (no-op for PySpark)"""
        pass

def create_connection():
    """Create a PySpark DataFrame connection"""
    try:
        spark = SparkSession.builder.appName("EnhancedTableReportGenerator").getOrCreate()
        connection = DataFrameConnection(spark)
        return connection
    except Exception as e:
        print(f"❌ Failed to create PySpark session: {str(e)}")
        return None

def format_table_name(table_name):
    """Format table name for Unity Catalog (catalog.schema.table)"""
    return f"{CATALOG_NAME}.{table_name}"

# =============================================================================
# PROGRESS TRACKING UTILITIES
# =============================================================================

class ProgressTracker:
    """Track progress for long operations"""
    def __init__(self, total_items, description="Processing"):
        self.total = total_items
        self.current = 0
        self.description = description
        self.start_time = time.time()
        print(f"\n🚀 {description}: 0/{total_items} (0%)")
    
    def update(self, increment=1, item_name=""):
        """Update progress"""
        self.current += increment
        pct = (self.current / self.total) * 100
        elapsed = time.time() - self.start_time
        
        if self.current > 0:
            eta = (elapsed / self.current) * (self.total - self.current)
            eta_str = f"ETA: {eta:.0f}s" if eta > 0 else "ETA: done"
        else:
            eta_str = "ETA: calculating..."
        
        print(f"📊 {self.description}: {self.current}/{self.total} ({pct:.1f}%) - {item_name} - {eta_str}")
    
    def complete(self):
        """Mark as complete"""
        elapsed = time.time() - self.start_time
        print(f"✅ {self.description} completed in {elapsed:.1f}s")

# =============================================================================
# PROPER DATABRICKS EXCEPTION HANDLING
# =============================================================================

def handle_table_access_error(error, table_name):
    """
    Proper Databricks/PySpark exception handling
    """
    error_str = str(error)
    
    # Handle specific PySpark/Databricks exceptions
    if isinstance(error, AnalysisException):
        if "TABLE_OR_VIEW_NOT_FOUND" in error_str:
            return "Table does not exist", False
        elif "SCHEMA_NOT_FOUND" in error_str:
            return "Schema does not exist", False
        elif any(perm_key in error_str.upper() for perm_key in ["PERMISSION_DENIED", "ACCESS_DENIED", "INSUFFICIENT_PERMISSIONS"]):
            return "Permission denied", True
        elif "INVALID_CATALOG" in error_str:
            return "Invalid catalog", False
        else:
            return f"Analysis error: {error_str}", False
    
    # Handle other common exceptions
    elif "timeout" in error_str.lower():
        return "Query timeout", True
    elif "connection" in error_str.lower():
        return "Connection error", True
    elif "memory" in error_str.lower():
        return "Memory error", True
    else:
        return f"Unknown error: {error_str}", False

# =============================================================================
# BATCH METADATA QUERIES FOR PERFORMANCE
# =============================================================================

def get_batch_table_metadata(connection, table_list):
    """
    Get metadata for multiple tables in batch operations
    Much more efficient than individual queries
    """
    print(f"\n📊 Getting batch metadata for {len(table_list)} tables...")
    
    # Prepare batch queries
    full_table_names = [format_table_name(table) for table, _ in table_list]
    
    # Initialize results
    batch_metadata = {}
    for table_name, desc in table_list:
        batch_metadata[table_name] = {
            'description': desc,
            'accessible': False,
            'exists': False,
            'error': None,
            'columns': [],
            'column_count': 0,
            'row_count': None,
            'creation_date': None,
            'last_update_date': None,
            'table_properties': {},
            'sample_data': None
        }
    
    # Try batch INFORMATION_SCHEMA query first (most efficient)
    try:
        catalog_tables_query = f"""
        SELECT 
            table_catalog,
            table_schema, 
            table_name,
            CONCAT(table_catalog, '.', table_schema, '.', table_name) as full_name
        FROM {CATALOG_NAME}.INFORMATION_SCHEMA.TABLES 
        WHERE table_catalog = '{CATALOG_NAME}'
        """
        
        existing_tables_df = connection.sql_to_pandas(catalog_tables_query)
        existing_tables = set(existing_tables_df['full_name'].tolist())
        
        print(f"📋 Found {len(existing_tables)} tables in catalog via INFORMATION_SCHEMA")
        
    except Exception as e:
        print(f"⚠️ INFORMATION_SCHEMA query failed: {str(e)}")
        print("📋 Falling back to individual table checks...")
        existing_tables = set()
    
    # Process each table with progress tracking
    progress = ProgressTracker(len(table_list), "Table metadata extraction")
    
    for table_name, description in table_list:
        full_table_name = format_table_name(table_name)
        result = batch_metadata[table_name]
        
        try:
            # Quick existence check
            if existing_tables and full_table_name not in existing_tables:
                result['error'] = "Table does not exist"
                progress.update(item_name=f"{table_name} - not found")
                continue
            
            # Get table description
            try:
                desc_df = connection.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}")
                desc_pandas = desc_df.toPandas()
                
                result['exists'] = True
                result['accessible'] = True
                
                # Extract columns (stop at metadata section)
                columns = []
                for _, row in desc_pandas.iterrows():
                    col_name = row.iloc[0] if pd.notna(row.iloc[0]) else ""
                    if col_name and not col_name.startswith('#') and col_name != '':
                        if col_name in ['# Detailed Table Information', '# Metadata Information']:
                            break
                        columns.append(col_name)
                
                result['columns'] = columns
                result['column_count'] = len(columns)
                
                # Extract metadata from extended description
                metadata_section = False
                for _, row in desc_pandas.iterrows():
                    col_name = row.iloc[0] if pd.notna(row.iloc[0]) else ""
                    data_type = row.iloc[1] if len(row) > 1 and pd.notna(row.iloc[1]) else ""
                    
                    if col_name == '# Detailed Table Information':
                        metadata_section = True
                        continue
                    
                    if metadata_section and col_name:
                        if 'created' in col_name.lower():
                            result['creation_date'] = data_type
                        elif any(keyword in col_name.lower() for keyword in ['last_ddl', 'modified', 'updated']):
                            result['last_update_date'] = data_type
                
            except Exception as desc_error:
                error_msg, exists = handle_table_access_error(desc_error, table_name)
                result['error'] = error_msg
                result['exists'] = exists
                progress.update(item_name=f"{table_name} - {error_msg}")
                continue
            
            # Get row count (separate try-catch for performance)
            try:
                count_df = connection.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}")
                count_result = count_df.collect()[0]
                result['row_count'] = count_result['cnt']
            except Exception as count_error:
                result['row_count'] = f"Count failed: {str(count_error)[:50]}"
            
            # Get table properties (separate try-catch)
            try:
                props_df = connection.sql(f"SHOW TBLPROPERTIES {full_table_name}")
                props_pandas = props_df.toPandas()
                
                properties = {}
                for _, prop_row in props_pandas.iterrows():
                    if len(prop_row) >= 2:
                        key, value = prop_row.iloc[0], prop_row.iloc[1]
                        properties[key] = value
                        
                        # Extract dates from properties
                        if key.lower() in ['created_time', 'createtime'] and not result['creation_date']:
                            result['creation_date'] = value
                        elif key.lower() in ['last_ddl_time', 'last_modified_time'] and not result['last_update_date']:
                            result['last_update_date'] = value
                
                result['table_properties'] = properties
                
            except Exception as props_error:
                # Table properties failure is not critical
                pass
            
            # Get sample data (separate try-catch)
            try:
                sample_df = connection.sql(f"SELECT * FROM {full_table_name} LIMIT 5")
                result['sample_data'] = sample_df.toPandas()
            except Exception as sample_error:
                # Sample data failure is not critical
                pass
            
            progress.update(item_name=f"{table_name} - ✅ accessible")
            
        except Exception as general_error:
            error_msg, exists = handle_table_access_error(general_error, table_name)
            result['error'] = error_msg
            result['exists'] = exists
            progress.update(item_name=f"{table_name} - {error_msg}")
    
    progress.complete()
    return batch_metadata

# =============================================================================
# ENHANCED FIELD ANALYSIS FUNCTIONS
# =============================================================================

def identify_primary_key_candidates(columns):
    """Identify potential primary key fields using pattern matching"""
    if not columns:
        return []
    
    primary_key_patterns = [
        r'.*_id$',           # ends with _id
        r'.*_key$',          # ends with _key
        r'^id$',             # exactly 'id'
        r'^key$',            # exactly 'key'
        r'.*_pk$',           # ends with _pk
        r'.*_uuid$',         # ends with _uuid
        r'.*_guid$',         # ends with _guid
        r'emp_id',           # employee id variations
        r'employee_id',
        r'client_id',
        r'person_id',
        r'work_id'
    ]
    
    candidates = []
    for col in columns:
        col_lower = col.lower()
        for pattern in primary_key_patterns:
            if re.match(pattern, col_lower):
                candidates.append(col)
                break
    
    return list(set(candidates))

def identify_date_fields(columns):
    """Identify date/time fields using pattern matching"""
    if not columns:
        return []
    
    date_patterns = [
        r'.*date.*',         # contains 'date'
        r'.*time.*',         # contains 'time'
        r'.*created.*',      # contains 'created'
        r'.*updated.*',      # contains 'updated'
        r'.*modified.*',     # contains 'modified'
        r'.*timestamp.*',    # contains 'timestamp'
        r'.*start.*',        # contains 'start'
        r'.*end.*',          # contains 'end'
        r'.*effective.*',    # contains 'effective'
        r'.*expire.*',       # contains 'expire'
    ]
    
    date_fields = []
    for col in columns:
        col_lower = col.lower()
        for pattern in date_patterns:
            if re.match(pattern, col_lower):
                date_fields.append(col)
                break
    
    return list(set(date_fields))

def identify_important_fields(columns):
    """Identify other important business fields"""
    if not columns:
        return []
    
    important_patterns = [
        r'.*name.*',         # name fields
        r'.*status.*',       # status fields
        r'.*type.*',         # type fields
        r'.*code.*',         # code fields
        r'.*amount.*',       # amount fields
        r'.*salary.*',       # salary fields
        r'.*rate.*',         # rate fields
        r'.*count.*',        # count fields
        r'.*flag.*',         # flag fields
        r'.*indicator.*',    # indicator fields
    ]
    
    important_fields = []
    for col in columns:
        col_lower = col.lower()
        for pattern in important_patterns:
            if re.match(pattern, col_lower):
                important_fields.append(col)
                break
    
    return list(set(important_fields))

# =============================================================================
# BUSINESS CONTEXT MAPPING
# =============================================================================

def get_business_context(schema_name, table_name):
    """Map tables to business context and ETL pipeline stages"""
    context = {
        'pipeline_stage': 'Unknown',
        'data_source': 'Unknown',
        'business_purpose': 'Unknown',
        'migration_priority': 'Low'
    }
    
    # ETL Pipeline Stage Mapping
    if 'blue_landing_base' in schema_name:
        context['pipeline_stage'] = 'Landing Base (Raw Ingestion)'
        context['data_source'] = 'Benchmarks System'
        context['migration_priority'] = 'High'
    elif 'blue_landing_main' in schema_name:
        context['pipeline_stage'] = 'Landing Main (Processed)'
        context['data_source'] = 'Benchmarks System'
        context['migration_priority'] = 'High'
    elif 'blue_landing' in schema_name:
        context['pipeline_stage'] = 'Landing (Initial)'
        context['data_source'] = 'Benchmarks System'
        context['migration_priority'] = 'High'
    elif 'blue_raw' in schema_name:
        context['pipeline_stage'] = 'Raw Data Warehouse'
        context['data_source'] = 'Analytics Warehouse'
        context['migration_priority'] = 'Medium'
    elif 'adpa' in schema_name:
        context['pipeline_stage'] = 'Target ADPA'
        context['data_source'] = 'ADPA Warehouse'
        context['migration_priority'] = 'Critical'
    elif 'oneai' in schema_name:
        context['pipeline_stage'] = 'Output/Processing'
        context['data_source'] = 'Generated/Processed'
        context['migration_priority'] = 'Low'
    
    # Business Purpose Mapping
    if any(keyword in table_name.lower() for keyword in ['employee', 'person', 'pers']):
        context['business_purpose'] = 'Employee/Person Data'
    elif any(keyword in table_name.lower() for keyword in ['client', 'clnt']):
        context['business_purpose'] = 'Client Data'
    elif any(keyword in table_name.lower() for keyword in ['work', 'job', 'assignment']):
        context['business_purpose'] = 'Work Assignment Data'
    elif any(keyword in table_name.lower() for keyword in ['payroll', 'payrl', 'earn']):
        context['business_purpose'] = 'Payroll/Earnings Data'
    elif any(keyword in table_name.lower() for keyword in ['turnover', 'trnovr']):
        context['business_purpose'] = 'Turnover Prediction'
    elif any(keyword in table_name.lower() for keyword in ['hierarchy', 'hrchy', 'manager', 'mngr']):
        context['business_purpose'] = 'Organizational Hierarchy'
    elif any(keyword in table_name.lower() for keyword in ['dim_', 'dimension']):
        context['business_purpose'] = 'Dimension Table'
    elif any(keyword in table_name.lower() for keyword in ['fact_']):
        context['business_purpose'] = 'Fact Table'
    
    return context

# =============================================================================
# TABLE DEFINITIONS
# =============================================================================

tables_to_check = {
    # Current Quarterly Tables (Section 2.3)
    "quarterly_tables": [
        ("us_east_1_prd_ds_blue_landing.ap_monthly", "Analytics Postings Monthly"),
        ("us_east_1_prd_ds_blue_landing.dw_t_clnt_prfl_setting", "Client Profile Settings"),
        ("us_east_1_prd_ds_blue_landing.optout_master", "Opt-out Master"),
        ("us_east_1_prd_ds_blue_landing_base.client_master", "Client Master"),
        ("us_east_1_prd_ds_blue_landing_base.employee_base_monthly", "Employee Base Monthly"),
        ("us_east_1_prd_ds_blue_landing_base.industry_hierarchy_us", "Industry Hierarchy US"),
        ("us_east_1_prd_ds_blue_landing_base.nas_job", "NAS Job"),
        ("us_east_1_prd_ds_blue_landing_base.nas_person", "NAS Person"),
        ("us_east_1_prd_ds_blue_landing_base.nas_work_assignment", "NAS Work Assignment"),
        ("us_east_1_prd_ds_blue_landing_base.nas_work_event", "NAS Work Event"),
        ("us_east_1_prd_ds_blue_landing_base.nas_work_event_type", "NAS Work Event Type"),
        ("us_east_1_prd_ds_blue_landing_base.nas_work_location", "NAS Work Location"),
        ("us_east_1_prd_ds_blue_landing_base.nas_work_person", "NAS Work Person"),
        ("us_east_1_prd_ds_blue_landing_base.term_nas_output", "Termination NAS Output"),
        ("us_east_1_prd_ds_blue_landing_base.term_wfn_output", "Termination WFN Output"),
        ("us_east_1_prd_ds_blue_landing_base.wfn_person", "WFN Person"),
        ("us_east_1_prd_ds_blue_landing_base.wfn_work_assignment", "WFN Work Assignment"),
        ("us_east_1_prd_ds_blue_landing_main.employee_main_monthly", "Employee Main Monthly"),
        ("us_east_1_prd_ds_main.t_dim_client_industries", "Client Industries Dimension"),
        ("us_east_1_prd_ds_main.t_dim_day", "Day Dimension"),
        ("us_east_1_prd_ds_main.top_tmp_t_dim_qtr", "Quarterly Dimension Temp"),
        ("us_east_1_prd_ds_raw.meta_t_dim_known_commutes", "Known Commutes Metadata"),
        ("us_east_1_prd_ds_raw.meta_t_dim_state_prov", "State Province Metadata"),
        ("us_east_1_prd_ds_raw.meta_t_dim_zip_lat_long", "ZIP Lat/Long Metadata"),
    ],
    
    # Current Weekly Tables (Section 2.3)
    "weekly_tables": [
        ("us_east_1_prd_ds_blue_raw.dwh_t_clnt_job_titl_mapping", "Client Job Title Mapping"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_dim_clnt", "Client Dimension"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_dim_job", "Job Dimension"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_dim_payrl_earn", "Payroll Earnings Dimension"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_dim_pers", "Person Dimension"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_dim_pers_prfl_attr", "Person Profile Attributes"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_dim_work_event", "Work Event Dimension"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_dim_work_loc", "Work Location Dimension"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_fact_payrl_earn_dtl", "Payroll Earnings Detail Fact"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_fact_work_asgnmt", "Work Assignment Fact"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_fact_work_event", "Work Event Fact"),
        ("us_east_1_prd_ds_blue_raw.dwh_t_rpt_to_hrchy", "Reporting Hierarchy"),
        ("us_east_1_prd_ds_raw.dwh_t_dim_geo_info", "Geographic Info Dimension"),
    ],
    
    # New ADPA Tables (Section 3.2.1)
    "adpa_tables": [
        ("adpa.t_fact_work_asgnmt", "Work Assignment Fact - ADPA"),
        ("adpa.t_fact_work_event", "Work Event Fact - ADPA"),
        ("adpa.t_dim_pers", "Person Dimension - ADPA"),
        ("adpa.t_clnt_job_titl_mapping", "Client Job Title Mapping - ADPA"),
        ("adpa.t_dim_pers_prfl_attr", "Person Profile Attributes - ADPA"),
        ("adpa.t_mngr_hrchy_sec", "Manager Hierarchy Security - ADPA"),
        ("adpa.t_supvr_hrchy_sec", "Supervisor Hierarchy Security - ADPA"),
    ],
    
    # Benchmarks Tables (Section 3.2.1)
    "benchmarks_tables": [
        ("us_east_1_prd_ds_blue_landing.optout_master", "Opt-out Master - Benchmarks"),
        ("us_east_1_prd_ds_blue_landing_base.client_master", "Client Master - Benchmarks"),
        ("us_east_1_prd_ds_blue_landing_main.employee_main_monthly", "Employee Main Monthly - Benchmarks"),
    ],
    
    # Output Tables (Section 2.2)
    "output_tables": [
        ("oneai.t_empl_trnovr_prblty", "Employee Turnover Probability"),
        ("oneai.top_dataset_combined", "TOP Dataset Combined"),
        ("oneai.top_risk_mngr_hierarchy_input", "TOP Risk Manager Hierarchy Input"),
        ("oneai.top_risk_mngr_hierarchy", "TOP Risk Manager Hierarchy"),
        ("oneai.top_processed_dataset_test_v2", "TOP Processed Dataset Test V2"),
        ("oneai.top_dw_extract_trh", "TOP DW Extract TRH"),
        ("oneai.top_batch_output_mlops", "TOP Batch Output MLOps"),
        ("oneai.t_empl_trnovr_fctr", "Employee Turnover Factor"),
        ("oneai.top_batch_output", "TOP Batch Output"),
        ("oneai.top_weekly_output", "TOP Weekly Output"),
        ("oneai.top_wfn_layers", "TOP WFN Layers"),
        ("oneai.top_processed_dataset_trimmed", "TOP Processed Dataset Trimmed"),
        ("oneai.top_nas_layers", "TOP NAS Layers"),
        ("oneai.top_processed_dataset_test", "TOP Processed Dataset Test"),
        ("oneai.top_dw_processed_dataset", "TOP DW Processed Dataset"),
    ]
}

# =============================================================================
# ENHANCED REPORT GENERATION
# =============================================================================

def format_number(num):
    """Format numbers with commas"""
    if isinstance(num, (int, float)) and not isinstance(num, str):
        return f"{num:,}"
    return str(num) if num is not None else "-"

def create_table_link(table_name, accessible):
    """Create markdown link for accessible tables"""
    if accessible:
        link_id = table_name.replace('.', '-').replace('_', '-')
        return f"[{table_name}](#{link_id})"
    return table_name

def generate_comments(result):
    """Generate comprehensive comments for each table"""
    comments = [result['description']]
    
    if not result['accessible']:
        comments.append(f"❌ {result['error']}")
        return " | ".join(comments)
    
    # Add business context
    business_context = get_business_context(result.get('schema_name', ''), result.get('actual_table_name', ''))
    comments.append(f"🏗️ {business_context['pipeline_stage']}")
    comments.append(f"📊 {business_context['business_purpose']}")
    
    # Add field analysis
    field_info = []
    primary_keys = identify_primary_key_candidates(result['columns'])
    date_fields = identify_date_fields(result['columns'])
    
    if primary_keys:
        field_info.append(f"🔑 Key fields: {', '.join(primary_keys[:3])}")
    if date_fields:
        field_info.append(f"📅 Date fields: {', '.join(date_fields[:3])}")
    
    if field_info:
        comments.extend(field_info)
    
    # Add migration priority
    priority = business_context['migration_priority']
    if priority == 'Critical':
        comments.append("🚨 Critical migration priority")
    elif priority == 'High':
        comments.append("⚡ High migration priority")
    
    return " | ".join(comments)

def run_enhanced_table_check():
    """Run the enhanced table access check with batch processing"""
    print(f"\n🔍 Starting enhanced table access check with batch processing...")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    connection = create_connection()
    if not connection:
        print("❌ Cannot proceed without PySpark session")
        return [], []
    
    try:
        # Flatten all tables into single list for batch processing
        all_tables = []
        for category, table_list in tables_to_check.items():
            all_tables.extend(table_list)
        
        print(f"📊 Processing {len(all_tables)} tables across {len(tables_to_check)} categories")
        
        # Get batch metadata
        batch_results = get_batch_table_metadata(connection, all_tables)
        
        # Convert to enhanced results format
        all_results = []
        accessible_results = []
        
        for table_name, metadata in batch_results.items():
            # Parse catalog, schema, table from full name
            parts = table_name.split('.')
            schema_name = parts[0] if len(parts) > 1 else 'unknown'
            actual_table_name = parts[-1]
            
            result = {
                'table_name': table_name,
                'actual_table_name': actual_table_name,
                'schema_name': schema_name,
                'catalog_name': CATALOG_NAME,
                'full_table_name': format_table_name(table_name),
                'description': metadata['description'],
                'accessible': metadata['accessible'],
                'exists': metadata['exists'],
                'row_count': metadata['row_count'],
                'column_count': metadata['column_count'],
                'columns': metadata['columns'],
                'sample_data': metadata['sample_data'],
                'creation_date': metadata['creation_date'],
                'last_update_date': metadata['last_update_date'],
                'table_properties': metadata['table_properties'],
                'error': metadata['error'],
                'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            all_results.append(result)
            if result['accessible']:
                accessible_results.append(result)
        
        # Print summary by category
        print(f"\n📊 SUMMARY BY CATEGORY:")
        for category, table_list in tables_to_check.items():
            category_accessible = sum(1 for table, _ in table_list 
                                    if batch_results[table]['accessible'])
            total_in_category = len(table_list)
            print(f"   {category}: {category_accessible}/{total_in_category} accessible")
    
    finally:
        connection.close()
    
    return all_results, accessible_results

def generate_enhanced_reports(all_results, accessible_results):
    """Generate comprehensive reports including main table report"""
    print(f"\n\n{'='*80}")
    print("📋 GENERATING ENHANCED REPORTS")
    print(f"{'='*80}")
    
    # Sort results: accessible first, then by schema, then by table name
    sorted_results = sorted(all_results, key=lambda x: (
        not x['accessible'],  # Accessible tables first
        x['schema_name'],     # Then by schema
        x['actual_table_name'] # Then by table name
    ))
    
    # Generate main report table
    report_rows = []
    for i, result in enumerate(sorted_results, 1):
        
        # Format dates
        creation_date = result['creation_date'][:10] if result['creation_date'] and len(result['creation_date']) > 10 else (result['creation_date'] or '-')
        last_update = result['last_update_date'][:10] if result['last_update_date'] and len(result['last_update_date']) > 10 else (result['last_update_date'] or '-')
        
        # Generate primary key string
        primary_keys = identify_primary_key_candidates(result['columns'])
        primary_key_str = ', '.join(primary_keys[:3]) if primary_keys else '-'
        
        row = {
            'S.N.': i,
            'Table': create_table_link(result['table_name'], result['accessible']),
            'Catalog': result['catalog_name'],
            'Schema': result['schema_name'],
            'Source': 'confluence',
            'Accessible_16June': 'Yes' if result['accessible'] else 'No',
            'Total Records': format_number(result['row_count']) if result['accessible'] else '-',
            'Columns': format_number(result['column_count']) if result['accessible'] else '-',
            'Creation Date': creation_date,
            'Last Update Date': last_update,
            'Primary Key': primary_key_str,
            'Comments': generate_comments(result)
        }
        report_rows.append(row)
    
    # Create main report DataFrame
    report_df = pd.DataFrame(report_rows)
    
    # Generate markdown table
    markdown_content = "# ADP Turnover Probability - Enhanced Table Access Report (Fixed)\n\n"
    markdown_content += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    markdown_content += f"**Unity Catalog:** {CATALOG_NAME}\n\n"
    markdown_content += f"**Fixes Applied:** Consistent DataFrame pattern, Proper exception handling, Batch queries, Progress tracking\n\n"
    
    # Summary statistics
    total_tables = len(all_results)
    accessible_count = len(accessible_results)
    
    markdown_content += f"## Summary Statistics\n\n"
    markdown_content += f"- **Total Tables Checked:** {total_tables}\n"
    markdown_content += f"- **Accessible Tables:** {accessible_count} ({accessible_count/total_tables*100:.1f}%)\n"
    markdown_content += f"- **Tables with Access Issues:** {len([r for r in all_results if r['exists'] and not r['accessible']])}\n"
    markdown_content += f"- **Tables Not Found:** {len([r for r in all_results if not r['exists']])}\n\n"
    
    # Schema breakdown
    schema_stats = defaultdict(lambda: {'total': 0, 'accessible': 0})
    for result in all_results:
        schema = result['schema_name']
        schema_stats[schema]['total'] += 1
        if result['accessible']:
            schema_stats[schema]['accessible'] += 1
    
    markdown_content += f"## Schema Breakdown\n\n"
    for schema, stats in sorted(schema_stats.items()):
        pct = (stats['accessible'] / stats['total'] * 100) if stats['total'] > 0 else 0
        markdown_content += f"- **{schema}:** {stats['accessible']}/{stats['total']} accessible ({pct:.1f}%)\n"
    
    markdown_content += f"\n## Main Report Table\n\n"
    markdown_content += report_df.to_markdown(index=False)
    
    # Repository and documentation links
    markdown_content += f"\n\n## Project References\n\n"
    markdown_content += f"- **Code Repository:** [turnoverprobabilityv2](https://bitbucket.es.ad.adp.com/projects/DSMAIN/repos/turnoverprobabilityv2/browse)\n"
    markdown_content += f"- **Legacy Repository:** [turnoverprobability](https://bitbucket.es.ad.adp.com/projects/DSMAIN/repos/turnoverprobability/browse)\n"
    markdown_content += f"- **Documentation:** [Confluence Page](https://confluence.page.url)\n"
    markdown_content += f"- **Feature Mapping:** [Column Mapping Spreadsheet](https://spreadsheet.url)\n\n"
    
    # Save main report
    with open('enhanced_table_access_report_fixed.md', 'w', encoding='utf-8') as f:
        f.write(markdown_content)
    
    # Save CSV
    report_df.to_csv('enhanced_table_access_report_fixed.csv', index=False)
    
    print(f"\n📊 REPORT GENERATION COMPLETED")
    print(f"   Enhanced report: enhanced_table_access_report_fixed.md")
    print(f"   CSV export: enhanced_table_access_report_fixed.csv")
    
    print(f"\n✅ ACCESSIBLE TABLES ({len(accessible_results)}):")
    for result in accessible_results[:10]:  # Show first 10
        print(f"   - {result['table_name']} ({format_number(result['column_count'])} cols, {format_number(result['row_count'])} rows)")
    if len(accessible_results) > 10:
        print(f"   ... and {len(accessible_results) - 10} more")
    
    return report_df

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print(f"\n🚀 STARTING ENHANCED TABLE ACCESS REPORT GENERATION (FIXED)")
    print(f"This will check {sum(len(tables) for tables in tables_to_check.values())} tables with enhanced metadata...")
    
    try:
        all_results, accessible_results = run_enhanced_table_check()
        report_df = generate_enhanced_reports(all_results, accessible_results)
        
        print(f"\n🎯 ENHANCED TABLE REPORT COMPLETED (FIXED)")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"\n💡 Critical Fixes Applied:")
        print(f"   ✅ Consistent DataFrame pattern (no more cursor/tuple inconsistency)")
        print(f"   ✅ Proper Databricks exception handling with AnalysisException")
        print(f"   ✅ Batch metadata queries using INFORMATION_SCHEMA")
        print(f"   ✅ Progress tracking for long operations")
        
        print(f"\n📁 Next Steps:")
        print(f"   1. Review enhanced_table_access_report_fixed.md for the main report")
        print(f"   2. Use CSV for further analysis and integration")
        print(f"   3. Share with ADP team for access resolution")
        
    except Exception as e:
        print(f"\n❌ Error during execution: {str(e)}")
        print(f"💡 Check your PySpark session and Unity Catalog permissions")
