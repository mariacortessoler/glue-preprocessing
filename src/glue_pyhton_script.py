
print('Loading the required Python libraries')
import pandas as pd
import awswrangler as wr
import ast
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
import gc
import boto3
import sys

print('Reading the AWS Glue job parameters')
args = getResolvedOptions(sys.argv,['today', 'data_bucket_name'])
today = datetime.strptime(args['today'], '%Y-%m-%d').date()
data_bucket_name = args['data_bucket_name']
first_day_current_month=today.replace(day=1)
last_day_previous_month=(first_day_current_month - timedelta(days=1))

print('Reading the two transactions types data')
transactions_type1=wr.s3.read_parquet(path=f's3://{data_bucket_name}/data/raw/transactions/type_1/dt={last_day_previous_month}')
transactions_type2=wr.s3.read_parquet(path=f's3://{data_bucket_name}/data/raw/transactions/type_2/dt={last_day_previous_month}')

print('Merging the two transactions types data')
accounts_=pd.Series(list(transactions_type1.id.unique())+list(transactions_type2.id.unique())).unique().astype('Int64')
df_transactions=( 
    pd.DataFrame({'id':list(accounts_)})
    .merge(transactions_type1,how='left',on='id')
    .merge(transactions_type2,how='left',on='id')
)

print('Delete temporal datasets no longer required')
del transactions_type1, transactions_type2, accounts_
gc.collect()
df_transactions.dropna(subset=['id'], inplace=True)
df_transactions['id']=pd.to_numeric(df_transactions['id'], errors='coerce').astype(np.int64)
df_transactions.fillna(0, inplace=True)

print('Read events data')
df_events=wr.s3.read_parquet(path=f's3://{data_bucket_name}/data/raw/events/dt={last_day_previous_month}')

print('Merging users and events data')
df_final=pd.merge(df_transactions, df_events, how='left', left_on='id', right_on='id')
del df_events
gc.collect()
#Fill missing values
df_final['fl_os_android']=df_final['fl_os_android'].fillna(1)
df_final['fl_os_ios']=df_final['fl_os_ios'].fillna(0)
df_final.fillna(0, inplace=True)
df_final['dt']=str(last_day_previous_month)

print('Save the final dataset in S3')
wr.s3.to_parquet(df_final,
                 path=f's3://{data_bucket_name}/data/monthly_stage/',
                 dataset=True,
                 partition_cols=['dt'],
                 mode="overwrite_partitions",
                 concurrent_partitioning=True,
                 index=False
                )  
