"""
ADP Turnover Probability - Database Table Access Checker for Databricks SQL Warehouse

This script checks access to all tables mentioned in the project documentation
Uses Databricks SQL connector (not PySpark)

SETUP INSTRUCTIONS:
1. Update the three connection variables below with your SQL Warehouse details
2. Ensure you have the databricks-sql-connector package installed
3. Run the script to check table access and generate reports
"""

from databricks import sql
import pandas as pd
import os
import logging
from datetime import datetime

# =============================================================================
# CONNECTION CONFIGURATION - UPDATE THESE VALUES
# =============================================================================
SERVER_HOSTNAME = ""
HTTP_PATH = ""
ACCESS_TOKEN = ""

# Unity Catalog configuration
CATALOG_NAME = "onedata_us_east_1_shared_dit"

# =============================================================================
# SCRIPT CONFIGURATION
# =============================================================================

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print(f"🎯 ADP Turnover Probability - Table Access Checker")
print(f"Using Databricks SQL Warehouse: {SERVER_HOSTNAME}")
print(f"Unity Catalog: {CATALOG_NAME}")
print("=" * 80)

def create_connection():
    """
    Create a connection to Databricks SQL Warehouse
    """
    try:
        connection = sql.connect(
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        )
        return connection
    except Exception as e:
        print(f"❌ Failed to connect to SQL Warehouse: {str(e)}")
        return None

def format_table_name(table_name):
    """
    Format table name for Unity Catalog (catalog.schema.table)
    """
    return f"{CATALOG_NAME}.{table_name}"

def check_table_access_sql(cursor, table_name, description=""):
    """
    Check if a table exists and is accessible using SQL cursor
    Returns a dictionary with access information
    """
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
        'sample_data': None,
        'error': None,
        'check_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    try:
        # First check if table exists and get basic info
        try:
            # Get table info
            info_query = f"DESCRIBE TABLE EXTENDED {full_table_name}"
            cursor.execute(info_query)
            table_info = cursor.fetchall()
            
            result['exists'] = True
            result['accessible'] = True
            
            # Extract column information
            columns = []
            for row in table_info:
                col_name = row[0] if row[0] else ""
                if col_name and not col_name.startswith('#') and col_name != '':
                    if col_name in ['# Detailed Table Information', '# Metadata Information']:
                        break
                    columns.append(col_name)
            
            result['columns'] = columns
            result['column_count'] = len(columns)
            
            # Try to get row count
            try:
                count_query = f"SELECT COUNT(*) as cnt FROM {full_table_name}"
                cursor.execute(count_query)
                count_result = cursor.fetchall()
                result['row_count'] = count_result[0][0] if count_result else 0
            except Exception as count_error:
                result['row_count'] = f"Count failed: {str(count_error)}"
            
            # Get sample data (top 5 rows)
            try:
                sample_query = f"SELECT * FROM {full_table_name} LIMIT 5"
                cursor.execute(sample_query)
                sample_data = cursor.fetchall()
                
                # Convert to pandas DataFrame
                if sample_data and columns:
                    # Use the columns we extracted from DESCRIBE TABLE
                    df_columns = columns[:len(sample_data[0])] if sample_data else columns
                    sample_df = pd.DataFrame(sample_data, columns=df_columns)
                    result['sample_data'] = sample_df
                else:
                    result['sample_data'] = None
                    
            except Exception as sample_error:
                result['sample_data'] = None
                
        except Exception as table_error:
            error_str = str(table_error)
            if "TABLE_OR_VIEW_NOT_FOUND" in error_str or "does not exist" in error_str.lower():
                result['error'] = "Table does not exist"
            elif "PERMISSION_DENIED" in error_str or "ACCESS_DENIED" in error_str or "permission" in error_str.lower():
                result['error'] = "Permission denied"
                result['exists'] = True
            elif "SCHEMA_NOT_FOUND" in error_str or "schema" in error_str.lower():
                result['error'] = "Schema does not exist"
            else:
                result['error'] = error_str
                
    except Exception as general_error:
        result['error'] = f"General error: {str(general_error)}"
    
    return result

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
    
    # Benchmarks Tables (Section 3.2.1)
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

def create_markdown_table_sample(table_name, pandas_df, description=""):
    """
    Create a markdown formatted table from pandas DataFrame
    """
    if pandas_df is None or pandas_df.empty:
        return f"### {table_name}\n**Description:** {description}\n*No data available*\n\n"
    
    # Limit column display to avoid overly wide tables
    max_cols = 8
    if len(pandas_df.columns) > max_cols:
        display_df = pandas_df.iloc[:, :max_cols].copy()
        truncated_note = f"\n*Note: Showing first {max_cols} of {len(pandas_df.columns)} columns*"
    else:
        display_df = pandas_df.copy()
        truncated_note = ""
    
    # Convert to string and limit cell content length
    for col in display_df.columns:
        display_df[col] = display_df[col].astype(str).apply(lambda x: x[:40] + "..." if len(str(x)) > 40 else x)
    
    # Create markdown table
    markdown = f"### {table_name}\n"
    markdown += f"**Description:** {description}\n"
    markdown += f"**Columns:** {len(pandas_df.columns)} | **Sample Rows:** {len(pandas_df)}\n\n"
    
    if len(display_df) > 0:
        markdown += display_df.to_markdown(index=False)
        markdown += truncated_note + "\n\n"
    else:
        markdown += "*No sample data available*\n\n"
    
    return markdown

def run_table_access_check():
    """
    Run the complete table access check
    """
    print(f"\n🔍 Starting table access check...")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create connection
    connection = create_connection()
    if not connection:
        print("❌ Cannot proceed without database connection")
        return [], []
    
    cursor = connection.cursor()
    all_results = []
    accessible_results = []
    
    try:
        for category, table_list in tables_to_check.items():
            print(f"\n{'='*60}")
            print(f"CHECKING {category.upper().replace('_', ' ')}")
            print(f"{'='*60}")
            
            category_results = []
            
            for table_name, description in table_list:
                print(f"\n📋 Checking: {table_name}")
                print(f"   Full path: {format_table_name(table_name)}")
                
                result = check_table_access_sql(cursor, table_name, description)
                category_results.append(result)
                all_results.append(result)
                
                # Print immediate result
                if result['accessible']:
                    print(f"   ✅ ACCESSIBLE - {result['column_count']} columns, {result['row_count']} rows")
                    accessible_results.append(result)
                elif result['exists']:
                    print(f"   ⚠️  EXISTS BUT NO ACCESS - {result['error']}")
                else:
                    print(f"   ❌ NOT ACCESSIBLE - {result['error']}")
            
            # Category summary
            accessible_count = sum(1 for r in category_results if r['accessible'])
            exists_count = sum(1 for r in category_results if r['exists'])
            total_count = len(category_results)
            
            print(f"\n📊 {category.upper()} SUMMARY:")
            print(f"   Accessible: {accessible_count}/{total_count}")
            print(f"   Exists: {exists_count}/{total_count}")
    
    finally:
        # Close connection
        cursor.close()
        connection.close()
    
    return all_results, accessible_results

def generate_reports(all_results, accessible_results):
    """
    Generate summary report and markdown with table samples
    """
    print(f"\n\n{'='*80}")
    print("📋 GENERATING REPORTS")
    print(f"{'='*80}")
    
    # Convert to DataFrame for analysis
    df = pd.DataFrame(all_results)
    
    # Overall statistics
    total_tables = len(all_results)
    accessible_count = len(accessible_results)
    existing_count = df['exists'].sum()
    
    print(f"\n📊 OVERALL STATISTICS:")
    print(f"   Total Tables Checked: {total_tables}")
    print(f"   Accessible Tables: {accessible_count} ({accessible_count/total_tables*100:.1f}%)")
    print(f"   Existing Tables: {existing_count} ({existing_count/total_tables*100:.1f}%)")
    
    # Save CSV report
    csv_data = []
    for result in all_results:
        csv_row = {k: v for k, v in result.items() if k != 'sample_data'}
        if result['columns']:
            csv_row['column_names'] = ', '.join(result['columns'])
        csv_data.append(csv_row)
    
    csv_df = pd.DataFrame(csv_data)
    csv_df.to_csv('table_access_report.csv', index=False)
    print(f"\n💾 CSV report saved to: table_access_report.csv")
    
    # Generate markdown report with samples
    if accessible_count > 0:
        print(f"\n📄 Generating markdown report with table samples...")
        
        markdown_content = "# ADP Turnover Probability - Table Access Report with Samples\n\n"
        markdown_content += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        markdown_content += f"**Unity Catalog:** {CATALOG_NAME}\n\n"
        markdown_content += f"**Summary:** {accessible_count} accessible tables out of {total_tables} checked\n\n"
        
        # Group accessible results by category
        categories = {}
        for result in accessible_results:
            table_name = result['table_name']
            if 'blue_landing_base' in table_name:
                category = 'Landing Base Tables'
            elif 'blue_landing_main' in table_name:
                category = 'Landing Main Tables'
            elif 'blue_landing' in table_name:
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
        
        # Generate markdown for each category
        for category, results in categories.items():
            markdown_content += f"## {category}\n\n"
            
            for result in results:
                table_markdown = create_markdown_table_sample(
                    result['table_name'],
                    result['sample_data'],
                    result['description']
                )
                markdown_content += table_markdown
        
        # Save markdown file
        with open('table_samples_report.md', 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        print(f"📋 Markdown report saved to: table_samples_report.md")
        print(f"   Contains top 5 rows from each accessible table")
    
    # Print accessible tables summary
    if accessible_count > 0:
        print(f"\n✅ ACCESSIBLE TABLES ({accessible_count}):")
        for result in accessible_results:
            print(f"   - {result['table_name']} ({result['column_count']} cols, {result['row_count']} rows)")
    
    # Print tables with issues
    no_access_results = [r for r in all_results if r['exists'] and not r['accessible']]
    if len(no_access_results) > 0:
        print(f"\n⚠️  TABLES WITH ACCESS ISSUES ({len(no_access_results)}):")
        for result in no_access_results:
            print(f"   - {result['table_name']}: {result['error']}")
    
    # Print tables not found
    not_found_results = [r for r in all_results if not r['exists']]
    if len(not_found_results) > 0:
        print(f"\n❌ TABLES NOT FOUND ({len(not_found_results)}):")
        for result in not_found_results:
            print(f"   - {result['table_name']}: {result['error']}")
    
    return df

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print(f"\n🚀 STARTING TABLE ACCESS CHECK")
    print(f"This will check {sum(len(tables) for tables in tables_to_check.values())} tables...")
    print(f"Connection: {SERVER_HOSTNAME}")
    
    try:
        all_results, accessible_results = run_table_access_check()
        summary_df = generate_reports(all_results, accessible_results)
        
        print(f"\n🎯 TABLE ACCESS CHECK COMPLETED")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\n📁 Files created:")
        print(f"   - table_access_report.csv")
        if len(accessible_results) > 0:
            print(f"   - table_samples_report.md")
        
        print(f"\n💡 Next Steps:")
        print(f"   1. Review the CSV file for detailed access information")
        print(f"   2. Check the markdown file for sample data from accessible tables")
        print(f"   3. Work with ADP team to resolve access issues for blocked tables")
        
    except Exception as e:
        print(f"\n❌ Error during execution: {str(e)}")
        print(f"💡 Check your connection parameters and Unity Catalog permissions")
