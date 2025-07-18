import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, current_timestamp, col, to_date, hour
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, timedelta
import logging
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Parse job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_CONNECTION_NAME',
    'TARGET_S3_BUCKET',
    'TABLE_CONFIG',      # JSON string with table configurations (can be empty)
    'INTERVAL_HOURS',    # Hours to look back (default 2)
    'BATCH_SIZE',
    'ENVIRONMENT'
])

JOB_NAME = args['JOB_NAME']
SOURCE_CONNECTION_NAME = args['SOURCE_CONNECTION_NAME']
TARGET_S3_BUCKET = args['TARGET_S3_BUCKET']
TABLE_CONFIG = json.loads(args.get('TABLE_CONFIG', '{}'))
INTERVAL_HOURS = int(args.get('INTERVAL_HOURS', '2'))
BATCH_SIZE = int(args.get('BATCH_SIZE', '10000'))
ENVIRONMENT = args.get('ENVIRONMENT', 'dev')

NOW = datetime.utcnow()
START_WINDOW = NOW - timedelta(hours=INTERVAL_HOURS)
START_WINDOW_STR = START_WINDOW.strftime('%Y-%m-%d %H:%M:%S')
END_WINDOW_STR = NOW.strftime('%Y-%m-%d %H:%M:%S')

# You can modify timestamp column names here if needed!
DEFAULT_TABLE_CONFIGS = {
    "docscan": {
        "incremental_column": "audit_timestamp", # This will be created in the query
        "primary_key": "systemgenerateid",
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                customerid,
                doccategory,
                docdesc,
                doctype,
                orderno,
                scan_date,
                scan_time,
                scan_user,
                audit_date,
                systemgenerateid,
                -- Combine audit_date and audit_time into timestamp
                CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) as audit_timestamp
            FROM docscan
            WHERE CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) >= '{start_time}'
              AND CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) < '{end_time}'
        """
    },
    
    "notesdtl": {
        "incremental_column": "audit_timestamp",
        "primary_key": "notesid",
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                cateid,
                created_date,
                created_time,
                created_user,
                notesdetail,
                noteskey1,
                notessubject,
                notestype,
                audit_date,
                notesid,
                audit_time,
                audit_user,
                noteskey2,
                CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) as audit_timestamp
            FROM notesdtl
            WHERE CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) >= '{start_time}'
              AND CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) < '{end_time}'
        """
    },
    
    "trackdtl": {
        "incremental_column": "audit_timestamp",
        "primary_key": "concat(trackno,'|',seqno)",
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                *,
                CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) as audit_timestamp
            FROM trackdtl
            WHERE CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) >= '{start_time}'
              AND CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) < '{end_time}'
        """
    },
    
    "claimdtladjust": {
        "incremental_column": "audit_timestamp",
        "primary_key": "systemgenerateid",
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                *,
                CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) as audit_timestamp
            FROM claimdtladjust
            WHERE CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) >= '{start_time}'
              AND CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) < '{end_time}'
        """
    },
    
    "claimhdr_hist": {
        "incremental_column": "audit_timestamp",
        "primary_key": "claimno",
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                *,
                CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) as audit_timestamp
            FROM claimhdr_hist
            WHERE CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) >= '{start_time}'
              AND CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) < '{end_time}'
              AND claimno > 0
        """
    },
    
    "th": {
        "incremental_column": "audit_timestamp",
        "primary_key": "sysid",
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                *,
                CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) as audit_timestamp
            FROM th
            WHERE CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) >= '{start_time}'
              AND CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) < '{end_time}'
        """
    },
    
    "taskconversation": {
        "incremental_column": "createddatetime", # This table already has a timestamp
        "primary_key": "conversationid",
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                conversationid,
                taskid,
                employeeid,
                detail,
                createddatetime,
                deleted,
                secretdetail,
                DATE(createddatetime) as audit_date,
                createddatetime as audit_timestamp
            FROM taskconversation
            WHERE createddatetime >= '{start_time}'
              AND createddatetime < '{end_time}'
        """
    },
    
    "claimdtl": {
        "incremental_column": "audit_timestamp",
        "primary_key": "claimdtlid", # Note: ADF uses claimno, but claimdtlid is more unique
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                *,
                CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) as audit_timestamp
            FROM claimdtl
            WHERE claimno IN (
                SELECT DISTINCT claimno
                FROM claimdtl
                WHERE CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) >= '{start_time}'
                  AND CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) < '{end_time}'
            )
        """
    },
    
    "acctledger": {
        "incremental_column": "audit_timestamp",
        "primary_key": "transactionid",
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                *,
                CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) as audit_timestamp
            FROM acctledger
            WHERE CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) >= '{start_time}'
              AND CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) < '{end_time}'
              AND transactionid NOT IN ('268845','2001949','2001950','4716609')
        """
    },
    
    "invtory": {
        "incremental_column": "audit_timestamp",
        "primary_key": "concat(sku,'|',office,'|',lotno)",
        "partition_columns": ["audit_date"],
        "query_template": """
            SELECT 
                *,
                CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) as audit_timestamp
            FROM invtory
            WHERE CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) >= '{start_time}'
              AND CAST(CONCAT(audit_date, ' ', COALESCE(audit_time, '00:00:00')) AS TIMESTAMP) < '{end_time}'
        """
    }
}

def get_jdbc_connection(glue_client, connection_name):
    try:
        conn = glue_client.get_connection(Name=connection_name)['Connection']
        conn_props = conn['ConnectionProperties']
        jdbc_url = conn_props['JDBC_CONNECTION_URL']
        if not jdbc_url.startswith('jdbc:'):
            jdbc_url = f"jdbc:postgresql://{jdbc_url}"
        return {
            'url': jdbc_url,
            'user': conn_props.get("USERNAME", ""),
            'password': conn_props.get("PASSWORD", "")
        }
    except Exception as e:
        logger.error(f"Failed to get connection details: {str(e)}")
        raise

def build_table_query(table_name, config, start_time, end_time):
    query_template = config.get('query_template')
    incremental_column = config.get('incremental_column')
    query = query_template.format(
        table_name=table_name,
        incremental_column=incremental_column,
        start_time=start_time,
        end_time=end_time
    )
    return f"({query}) as src"

def process_table_incremental(spark, conn_details, table_name, config, glue_context, start_time, end_time):
    logger.info(f"Processing table: {table_name}")

    try:
        query = build_table_query(table_name, config, start_time, end_time)
        df = spark.read.jdbc(
            url=conn_details['url'],
            table=query,
            properties={
                "user": conn_details['user'],
                "password": conn_details['password'],
                "driver": "org.postgresql.Driver",
                "fetchsize": str(BATCH_SIZE)
            }
        )
        record_count = df.count()
        logger.info(f"Records fetched for {table_name}: {record_count}")

        if record_count == 0:
            logger.info(f"No new data for table {table_name}. Skipping.")
            return 0

        # Add ETL metadata
        df = df.withColumn("etl_load_timestamp", current_timestamp()) \
               .withColumn("etl_job_name", lit(JOB_NAME)) \
               .withColumn("etl_source_system", lit("postgresql_bf")) \
               .withColumn("etl_table_name", lit(table_name))

        # Partition columns: only audit_date, but you can add 'audit_hour' if needed
        partition_columns = config.get('partition_columns', ['audit_date'])
        ts_col = config.get('incremental_column')
        if ts_col in df.columns and "audit_date" in partition_columns:
            df = df.withColumn("audit_date", to_date(col(ts_col)))
        if ts_col in df.columns and "created_date" in partition_columns:
            df = df.withColumn("created_date", to_date(col(ts_col)))

        dyf = DynamicFrame.fromDF(df, glue_context, f"dyf_{table_name}")
        output_path = f"s3://{TARGET_S3_BUCKET}/bf/bf_{table_name}/"

        glue_context.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="s3",
            connection_options={
                "path": output_path,
                "partitionKeys": partition_columns,
                "partitionOverwriteMode": "dynamic"
            },
            format="parquet",
            format_options={"compression": "snappy"},
            transformation_ctx=f"write_{table_name}_incremental"  # ← Add this line

        )

        logger.info(f"✅ Overwrote partition(s) for {table_name}: {record_count} records")
        logger.info(f"📁 Output location: {output_path}")

        return record_count

    except Exception as e:
        logger.error(f"❌ Failed to process table {table_name}: {str(e)}")
        raise

def main():
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(JOB_NAME, args)

    total_records_processed = 0
    successful_tables = []
    failed_tables = []

    try:
        glue_client = boto3.client('glue')
        conn_details = get_jdbc_connection(glue_client, SOURCE_CONNECTION_NAME)
        tables_to_process = {**DEFAULT_TABLE_CONFIGS, **TABLE_CONFIG}
        logger.info(f"🚀 Starting {INTERVAL_HOURS}-hour incremental load for {len(tables_to_process)} tables")
        logger.info(f"Time window: {START_WINDOW_STR} to {END_WINDOW_STR}")

        for table_name, config in tables_to_process.items():
            try:
                logger.info(f"🔄 Processing {table_name} (incremental strategy, {INTERVAL_HOURS}-hour window)")
                records_processed = process_table_incremental(
                    spark, conn_details, table_name, config, glue_context, START_WINDOW_STR, END_WINDOW_STR
                )
                total_records_processed += records_processed
                successful_tables.append(table_name)
                logger.info(f"✅ Successfully processed: {table_name}")
            except Exception as e:
                logger.error(f"❌ Failed processing {table_name}: {str(e)}")
                failed_tables.append(table_name)
                continue

        logger.info(f"Job completed!")
        logger.info(f"Total records processed: {total_records_processed}")
        logger.info(f"Successful tables ({len(successful_tables)}): {', '.join(successful_tables)}")
        if failed_tables:
            logger.error(f"Failed tables ({len(failed_tables)}): {', '.join(failed_tables)}")
        if failed_tables:
            raise Exception(f"Some tables failed to process: {failed_tables}")
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
