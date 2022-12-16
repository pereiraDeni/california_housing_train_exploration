from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import pyarrow.parquet as pq
import pyarrow

from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

var = {"columns_names": { "hma_cat": "age",
                         "c_ns": "california_region"}
}

columns = ["age", "california_region", "total_rooms", "total_bedrooms", "population", "households", "median_house_value"]

def upload_to_s3(date, filename, key, bucket, istrue):
    logger.info("Getting context from previous task")
    logger.info("Uploading to S3")

    S3Hook(aws_conn_id="aws").load_file(
        filename =filename + "_" + date + ".csv",
        key= key + date + ".csv",
        bucket_name=bucket,
        replace=True
    )
    if istrue:
        S3Hook(aws_conn_id="aws").load_file(
            filename =filename + "_" + date + ".parquet",
            key= key + date + ".parquet",
            bucket_name=bucket,
            replace=True
        )
    logger.info("Finished")

def transform_columns(date, filename_raw, filename_processed):
    logger.info("Initializing")

    sep=","
    file = filename_raw + "_" + date + ".csv"
    df = pd.read_csv(file, sep=sep)
    logger.info("DF Created")

    df["hma_cat"] = ["de_0_ate_18" if x < 18 
                        else "ate_29" if (x>=18 and x<29) 
                        else "ate_37" if (x>=29 and x<37) 
                        else "nao_informado" if pd.isna(x) 
                        else "acima_37" 
                        for x in df["housing_median_age"]]
    logger.info("HMA Created")

    df["c_ns"] = ["norte" if x < -119 
              else "nao_informado" if pd.isna(x) 
              else "sul" 
              for x in df["longitude"]]
    logger.info("C_NS Created")

    df.rename(columns = var["columns_names"], inplace = True)
    df = df[columns]
    logger.info("Renamed Columns")

    file = filename_processed + "_" + date + ".csv"
    df.to_csv(file, sep=sep)
    logger.info("Saved CSV")

    file = filename_processed + "_" + date + ".parquet"
    df.to_parquet(file, engine="pyarrow")
    logger.info("Saved Parquet")
    logger.info("Finished")

def validate_dtypes(date, filename):
    logger.info("Initializing")

    file = filename + "_" + date + ".parquet"
    df = pd.read_parquet(file)
    logger.info("DF Created")

    a = df.dtypes
    df = pd.DataFrame({'columns_name':a.index, 'dtypes':a.values})
    logger.info("DTypes Parquet Curated")

    df_validate = pd.read_csv("/usr/local/airflow/dags/file/validate_dtypes.csv")
    logger.info("DTypes Expected")

    if df_validate.equals(df) == False:
        import sys
        sys.exit()
    logger.info("OK!")

def agg_file_parquet(date, filename_processed, filename_curated):
    logger.info("Initializing")

    sep=","
    file = filename_processed + "_" + date + ".csv"
    df = pd.read_csv(file, sep=sep)
    logger.info("DF Created")

    def f(x):
        d = {}
        d["S_population"] = x["population"].sum()
        d["m_median_house_value"] = x["median_house_value"].mean()
        return pd.Series(d, index=["S_population", "m_median_house_value"])
    df_result = df.groupby(["age", "california_region"]).apply(f)
    logger.info("Agregated Columns")

    df_result = df_result.sort_values(by="m_median_house_value", ascending=False)
    logger.info("DESC Sorted Values")

    file = filename_curated + "_" + date + ".csv"
    df_result.to_csv(file, sep=sep)
    logger.info("Saved Final CSV")

    file = filename_curated + "_" + date + ".parquet"
    df_result.to_parquet(file, engine="pyarrow")
    logger.info("SAVED Final Parquet")
    logger.info("Finished")
