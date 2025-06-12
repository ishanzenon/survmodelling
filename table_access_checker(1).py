"""
ADP Turnover Probability - Database Table Access Checker
This script checks access to all tables mentioned in the project documentation
"""

import pandas as pd
from pyspark.sql import SparkSession
import logging
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("TableAccessChecker").getOrCreate()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_table_access(table_name, description=""):
    """
    Check if a table exists and is accessible
    Returns a dictionary with access information
    """
    # Format table name for Unity Catalog
    full_table_name = format_table_name(table_name)
    
    result = {
        'table_name': table_name,
        'full_table_name': full_table_name,
        'description': description,
        'accessible': False,
        'exists': False,
        'row_count': None,
        'column_count': None,
        'columns': None,
        'error': None,
        'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    try:
        # First check if table exists
        try:
            df = spark.table(full_table_name)
            result['exists'] = True
            result['accessible'] = True
            
            # Get basic info
            result['column_count'] = len(df.columns)
            result['columns'] = df.columns
            
            # Try to get row count (this might fail due to permissions)
            try:
                result['row_count'] = df.count()
            except Exception as count_error:
                result['row_count'] = f"Count failed: {str(count_error)}"
                
        except Exception as table_error:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(table_error):
                result['error'] = "Table does not exist"
            elif "PERMISSION_DENIED" in str(table_error):
                result['error'] = "Permission denied"
                result['exists'] = True  # Table exists but no access
            elif "SCHEMA_NOT_FOUND" in str(table_error):
                result['error'] = "Schema does not exist"
            else:
                result['error'] = str(table_error)
                
    except Exception as general_error:
        result['error'] = f"General error: {str(general_error)}"
    
    return result

# Unity Catalog configuration
CATALOG_NAME = "onedata_us_east_1_shared_dit"

def format_table_name(table_name):
    """
    Format table name for Unity Catalog (catalog.schema.table)
    """
    return f"{CATALOG_NAME}.{table_name}"

def discover_catalog_schemas():
    """
    Discover available schemas in the catalog
    """
    try:
        print(f"\n🔍 Discovering schemas in catalog: {CATALOG_NAME}")
        schemas_df = spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}")
        schemas = [row.databaseName for row in schemas_df.collect()]
        
        print(f"Found {len(schemas)} schemas:")
        for schema in sorted(schemas):
            print(f"  - {schema}")
        
        return schemas
    except Exception as e:
        print(f"❌ Could not discover schemas: {str(e)}")
        return []

def discover_schema_tables(schema_name, limit=10):
    """
    Discover tables in a specific schema (limited output)
    """
    try:
        full_schema = f"{CATALOG_NAME}.{schema_name}"
        tables_df = spark.sql(f"SHOW TABLES IN {full_schema}")
        tables = [row.tableName for row in tables_df.collect()]
        
        print(f"Schema {schema_name} has {len(tables)} tables")
        if len(tables) <= limit:
            for table in sorted(tables):
                print(f"    {table}")
        else:
            print(f"  First {limit} tables:")
            for table in sorted(tables)[:limit]:
                print(f"    {table}")
            print(f"    ... and {len(tables) - limit} more")
        
        return tables
    except Exception as e:
        print(f"❌ Could not discover tables in {schema_name}: {str(e)}")
        return []

def get_table_sample(full_table_name, limit=5):
    """
    Get a sample of rows from a table for markdown display
    """
    try:
        df = spark.sql(f"SELECT * FROM {full_table_name} LIMIT {limit}")
        pandas_df = df.toPandas()
        return pandas_df
    except Exception as e:
        return None

def create_markdown_table_sample(table_name, pandas_df):
    """
    Create a markdown formatted table from pandas DataFrame
    """
    if pandas_df is None or pandas_df.empty:
        return f"### {table_name}\n*No data available*\n\n"
    
    # Limit column display to avoid overly wide tables
    max_cols = 10
    if len(pandas_df.columns) > max_cols:
        display_df = pandas_df.iloc[:, :max_cols].copy()
        truncated_note = f"\n*Note: Showing first {max_cols} of {len(pandas_df.columns)} columns*"
    else:
        display_df = pandas_df.copy()
        truncated_note = ""
    
    # Convert to string and limit cell content length
    for col in display_df.columns:
        display_df[col] = display_df[col].astype(str).apply(lambda x: x[:50] + "..." if len(str(x)) > 50 else x)
    
    # Create markdown table
    markdown = f"### {table_name}\n\n"
    markdown += f"**Columns:** {len(pandas_df.columns)} | **Rows shown:** {len(pandas_df)}\n\n"
    markdown += display_df.to_markdown(index=False)
    markdown += truncated_note + "\n\n"
    
    return markdown

def generate_table_samples_markdown(accessible_results):
    """
    Generate markdown document with table samples for all accessible tables
    """
    markdown_content = "# ADP Turnover Probability - Table Access Report with Samples\n\n"
    markdown_content += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    markdown_content += f"**Unity Catalog:** {CATALOG_NAME}\n\n"
    
    # Group by category
    categories = {}
    for result in accessible_results:
        # Determine category based on table name patterns
        table_name = result['table_name']
        if 'blue_landing' in table_name:
            if 'blue_landing_base' in table_name:
                category = 'Landing Base Tables'
            elif 'blue_landing_main' in table_name:
                category = 'Landing Main Tables'
            else:
                category = 'Landing Tables'
        elif 'blue_raw' in table_name:
            category = 'Raw Tables'
        elif 'adpa' in table_name:
            category = 'ADPA Tables'
        elif table_name.startswith('top_') or 't_empl_trnovr' in table_name:
            category = 'Output/Processing Tables'
        else:
            category = 'Other Tables'
        
        if category not in categories:
            categories[category] = []
        categories[category].append(result)
    
    # Generate samples for each category
    for category, results in categories.items():
        markdown_content += f"## {category}\n\n"
        
        for result in results:
            print(f"📊 Getting sample data for: {result['table_name']}")
            
            # Get sample data
            sample_df = get_table_sample(result['full_table_name'])
            
            if sample_df is not None:
                table_markdown = create_markdown_table_sample(
                    f"{result['table_name']} ({result['description']})",
                    sample_df
                )
                markdown_content += table_markdown
            else:
                markdown_content += f"### {result['table_name']} ({result['description']})\n"
                markdown_content += "*Could not retrieve sample data*\n\n"
    
    return markdown_content

# Define all tables mentioned in the documentation
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
    
    # New ADPA Tables (Section 3.2.1) - These might be in a different schema
    "adpa_tables": [
        ("adpa.t_fact_work_asgnmt", "Work Assignment Fact - ADPA"),
        ("adpa.t_fact_work_event", "Work Event Fact - ADPA"),
        ("adpa.t_dim_pers", "Person Dimension - ADPA"),
        ("adpa.t_clnt_job_titl_mapping", "Client Job Title Mapping - ADPA"),
        ("adpa.t_dim_pers_prfl_attr", "Person Profile Attributes - ADPA"),
        ("adpa.t_mngr_hrchy_sec", "Manager Hierarchy Security - ADPA"),
        ("adpa.t_supvr_hrchy_sec", "Supervisor Hierarchy Security - ADPA"),
    ],
    
    # Benchmarks Tables (Section 3.2.1) - These might be in benchmarks schema
    "benchmarks_tables": [
        ("us_east_1_prd_ds_blue_landing.optout_master", "Opt-out Master - Benchmarks"),
        ("us_east_1_prd_ds_blue_landing_base.client_master", "Client Master - Benchmarks"),
        ("us_east_1_prd_ds_blue_landing_main.employee_main_monthly", "Employee Main Monthly - Benchmarks"),
    ],
    
    # Output Tables (Section 2.2) - These might be in your project schema
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

def run_access_check():
    """
    Run access check for all table categories
    """
    print("=== ADP Turnover Probability - Database Table Access Check ===")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n")
    
    all_results = []
    
    for category, table_list in tables_to_check.items():
        print(f"\n{'='*60}")
        print(f"CHECKING {category.upper().replace('_', ' ')}")
        print(f"{'='*60}")
        
        category_results = []
        
        for table_name, description in table_list:
            print(f"\nChecking: {table_name}")
            print(f"  Full path: {format_table_name(table_name)}")
            result = check_table_access(table_name, description)
            category_results.append(result)
            
            # Print immediate result
            if result['accessible']:
                print(f"  ✅ ACCESSIBLE - {result['column_count']} columns, {result['row_count']} rows")
            elif result['exists']:
                print(f"  ⚠️  EXISTS BUT NO ACCESS - {result['error']}")
            else:
                print(f"  ❌ NOT ACCESSIBLE - {result['error']}")
        
        all_results.extend(category_results)
        
        # Category summary
        accessible_count = sum(1 for r in category_results if r['accessible'])
        exists_count = sum(1 for r in category_results if r['exists'])
        total_count = len(category_results)
        
        print(f"\n{category.upper()} SUMMARY:")
        print(f"  Accessible: {accessible_count}/{total_count}")
        print(f"  Exists: {exists_count}/{total_count}")
    
    return all_results

def generate_summary_report(results):
    """
    Generate a summary report of all table access checks
    """
    print(f"\n\n{'='*80}")
    print("OVERALL SUMMARY REPORT")
    print(f"{'='*80}")
    
    # Convert to DataFrame for easier analysis
    df = pd.DataFrame(results)
    
    # Overall statistics
    total_tables = len(results)
    accessible_tables = df['accessible'].sum()
    existing_tables = df['exists'].sum()
    
    print(f"\nTotal Tables Checked: {total_tables}")
    print(f"Accessible Tables: {accessible_tables} ({accessible_tables/total_tables*100:.1f}%)")
    print(f"Existing Tables: {existing_tables} ({existing_tables/total_tables*100:.1f}%)")
    
    # Get accessible results for markdown generation
    accessible_results = df[df['accessible'] == True].to_dict('records')
    
    # Accessible tables
    if accessible_tables > 0:
        print(f"\n✅ ACCESSIBLE TABLES ({accessible_tables}):")
        accessible_df = df[df['accessible'] == True]
        for _, row in accessible_df.iterrows():
            print(f"  - {row['table_name']} ({row['column_count']} cols)")
            print(f"    Full path: {row['full_table_name']}")
    
    # Tables that exist but no access
    no_access_df = df[(df['exists'] == True) & (df['accessible'] == False)]
    if len(no_access_df) > 0:
        print(f"\n⚠️  TABLES WITH ACCESS ISSUES ({len(no_access_df)}):")
        for _, row in no_access_df.iterrows():
            print(f"  - {row['table_name']}: {row['error']}")
            print(f"    Full path: {row['full_table_name']}")
    
    # Tables that don't exist
    not_exist_df = df[df['exists'] == False]
    if len(not_exist_df) > 0:
        print(f"\n❌ TABLES NOT FOUND ({len(not_exist_df)}):")
        for _, row in not_exist_df.iterrows():
            print(f"  - {row['table_name']}: {row['error']}")
            print(f"    Full path: {row['full_table_name']}")
    
    # Generate CSV report for detailed analysis
    df.to_csv('table_access_report.csv', index=False)
    print(f"\n📄 Detailed report saved to: table_access_report.csv")
    
    # Generate markdown with table samples if we have accessible tables
    if accessible_tables > 0:
        print(f"\n📊 Generating markdown report with table samples...")
        markdown_content = generate_table_samples_markdown(accessible_results)
        
        # Save markdown file
        with open('table_samples_report.md', 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        print(f"📋 Table samples report saved to: table_samples_report.md")
        print(f"   This file contains top 5 rows from each accessible table")
    
    return df, accessible_results

# Main execution
if __name__ == "__main__":
    try:
        print(f"Using Unity Catalog: {CATALOG_NAME}")
        print("Table format: catalog.schema.table (matching DBeaver format)")
        print("=" * 80)
        
        # Optional: Run discovery first (uncomment the next lines if you want to explore)
        # print("\n🔍 DISCOVERY MODE - Exploring available schemas and tables...")
        # available_schemas = discover_catalog_schemas()
        # 
        # # Discover tables in key schemas
        # for schema in ['us_east_1_prd_ds_blue_landing', 'us_east_1_prd_ds_blue_landing_base', 'adpa']:
        #     if schema in available_schemas:
        #         discover_schema_tables(schema, limit=5)
        # 
        # print("\n" + "="*80)
        # input("Press Enter to continue with table access checks...")
        
        # Run the access check
        all_results = run_access_check()
        
        # Generate summary report and table samples
        summary_df, accessible_results = generate_summary_report(all_results)
        
        print(f"\n🎯 Access check completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Summary of deliverables
        print(f"\n📋 DELIVERABLES CREATED:")
        print(f"   1. table_access_report.csv - Detailed access check results")
        if len(accessible_results) > 0:
            print(f"   2. table_samples_report.md - Markdown with top 5 rows from each accessible table")
        
        # Quick tips for common issues
        print(f"\n💡 TROUBLESHOOTING TIPS:")
        print(f"   - If many tables show 'Schema does not exist', verify catalog name: {CATALOG_NAME}")
        print(f"   - If you see permission errors, check Unity Catalog grants")
        print(f"   - To explore available schemas, uncomment the discovery section above")
        print(f"   - ADPA tables might be in a different schema than assumed")
        print(f"   - Table format now matches DBeaver: {CATALOG_NAME}.schema.table")
        
    except Exception as e:
        logger.error(f"Error during table access check: {str(e)}")
        print(f"\n❌ Error during execution: {str(e)}")
        print(f"💡 Try uncommenting the discovery section to explore your catalog structure")
