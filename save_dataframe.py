import functools
from datetime import datetime
import os
from pyspark.sql import DataFrame
from typing import Callable, Optional, Union

def save_dataframe(
    save_path: str = "/tmp/dataframe_snapshots",
    save_format: str = "csv",
    save_mode: str = "overwrite",
    when: str = "after",  # "before", "after", or "both"
    include_timestamp: bool = True,
    coalesce_partitions: Optional[int] = 1,
    **save_options
):
    """
    Decorator to save PySpark DataFrames before/after transformation functions.
    
    Args:
        save_path: Base directory to save dataframes
        save_format: Format to save (csv, parquet, json, etc.)
        save_mode: Save mode (overwrite, append, ignore, error)
        when: When to save - "before", "after", or "both"
        include_timestamp: Whether to include timestamp in filename
        coalesce_partitions: Number of partitions to coalesce to (None to skip)
        **save_options: Additional options for the save operation
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(df: DataFrame, *args, **kwargs) -> DataFrame:
            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if include_timestamp else ""
            
            # Create function-specific directory
            func_name = func.__name__
            func_dir = os.path.join(save_path, func_name)
            
            def _save_df(dataframe: DataFrame, suffix: str = ""):
                """Helper function to save dataframe"""
                if include_timestamp:
                    filename = f"{func_name}_{timestamp}{suffix}"
                else:
                    filename = f"{func_name}{suffix}"
                
                file_path = os.path.join(func_dir, filename)
                
                # Coalesce if specified
                df_to_save = dataframe.coalesce(coalesce_partitions) if coalesce_partitions else dataframe
                
                # Save based on format
                if save_format.lower() == "csv":
                    df_to_save.write.mode(save_mode).option("header", "true").csv(file_path, **save_options)
                elif save_format.lower() == "parquet":
                    df_to_save.write.mode(save_mode).parquet(file_path, **save_options)
                elif save_format.lower() == "json":
                    df_to_save.write.mode(save_mode).json(file_path, **save_options)
                else:
                    # Generic write
                    df_to_save.write.mode(save_mode).format(save_format).save(file_path, **save_options)
                
                print(f"Saved dataframe to: {file_path}")
                return file_path
            
            # Save before transformation
            if when in ["before", "both"]:
                _save_df(df, "_before")
            
            # Execute the transformation
            result_df = func(df, *args, **kwargs)
            
            # Save after transformation
            if when in ["after", "both"]:
                _save_df(result_df, "_after" if when == "both" else "")
            
            return result_df
        
        return wrapper
    return decorator


# Example usage with your transform functions
@save_dataframe(
    save_path="/path/to/your/snapshots", 
    save_format="csv",
    when="after",
    coalesce_partitions=1
)
def lower_case_names():
    def transform_func(df: DataFrame) -> DataFrame:
        # Your transformation logic here
        return df.select([col(c).alias(c.lower()) for c in df.columns])
    return transform_func

@save_dataframe(
    save_path="/path/to/your/snapshots",
    save_format="csv", 
    when="after"
)
def filter_sources(sources):
    def transform_func(df: DataFrame) -> DataFrame:
        # Your transformation logic here
        return df.filter(col("source").isin(sources))
    return transform_func

@save_dataframe(
    save_path="/path/to/your/snapshots",
    save_format="csv",
    when="after"
)
def filter_live_primary():
    def transform_func(df: DataFrame) -> DataFrame:
        # Your transformation logic here
        return df.filter((col("status") == "live") & (col("primary") == True))
    return transform_func

# Alternative approach: Class-based decorator for more control
class DataFrameSaver:
    def __init__(self, base_path: str = "/tmp/dataframe_snapshots"):
        self.base_path = base_path
        self.saves_log = []
    
    def save_transform(self, save_format: str = "csv", when: str = "after", **options):
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(df: DataFrame, *args, **kwargs) -> DataFrame:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                func_name = func.__name__
                
                def _save(dataframe: DataFrame, suffix: str = ""):
                    filename = f"{func_name}_{timestamp}{suffix}"
                    file_path = os.path.join(self.base_path, func_name, filename)
                    
                    if save_format == "csv":
                        dataframe.coalesce(1).write.mode("overwrite").option("header", "true").csv(file_path)
                    elif save_format == "parquet":
                        dataframe.write.mode("overwrite").parquet(file_path)
                    
                    self.saves_log.append({
                        "function": func_name,
                        "timestamp": timestamp,
                        "path": file_path,
                        "row_count": dataframe.count()
                    })
                    print(f"Saved {func_name}: {file_path}")
                
                if when in ["before", "both"]:
                    _save(df, "_before")
                
                result_df = func(df, *args, **kwargs)
                
                if when in ["after", "both"]:
                    _save(result_df, "_after" if when == "both" else "")
                
                return result_df
            return wrapper
        return decorator
    
    def get_saves_summary(self):
        """Get summary of all saves performed"""
        return self.saves_log

# Usage with class-based approach
saver = DataFrameSaver("/path/to/snapshots")

@saver.save_transform(save_format="csv", when="after")
def timestamp_to_dates():
    def transform_func(df: DataFrame) -> DataFrame:
        # Your logic here
        return df.withColumn("date", to_date(col("timestamp")))
    return transform_func

# Your main pipeline remains clean
wass_base_df = (
    wass_f_raw_df.transform(lower_case_names())
    .transform(filter_sources(sources=SOR_SOURCES))
    .transform(filter_live_primary())
    .transform(timestamp_to_dates())
    .transform(with_clean_end_dates())
    .transform(with_clean_wass_terminations())
    .transform(with_weekly_episodes())
    .transform(with_forward_filled_job_cd())
    .transform(with_mgr_sup_id())
)

# Configuration class for centralized settings
class DataFrameSnapshotConfig:
    BASE_PATH = "/data/snapshots"
    SAVE_FORMAT = "parquet"  # More efficient than CSV for large datasets
    COALESCE_PARTITIONS = 1
    INCLUDE_METADATA = True
    
    @classmethod
    def get_decorator(cls, when: str = "after"):
        return save_dataframe(
            save_path=cls.BASE_PATH,
            save_format=cls.SAVE_FORMAT,
            when=when,
            coalesce_partitions=cls.COALESCE_PARTITIONS
        )

# Usage with config
@DataFrameSnapshotConfig.get_decorator(when="after")
def with_clean_end_dates():
    def transform_func(df: DataFrame) -> DataFrame:
        # Your logic here
        return df.withColumn("clean_end_date", when(col("end_date").isNull(), lit("9999-12-31")).otherwise(col("end_date")))
    return transform_func
