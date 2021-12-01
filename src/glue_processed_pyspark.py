
print('Loading the required libraries')
import sys
import pyspark.sql.functions as func
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
import json
import boto3
import ast
import datetime
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import gc
import sys
from pyspark.conf import SparkConf

print('Reading the AWS Glue job parameters')
args = getResolvedOptions(sys.argv,['today', 'data_bucket_name', 'months'])
today = datetime.strptime(args['today'], '%Y-%m-%d').date()
months = int(args['months'])
data_bucket_name = args['data_bucket_name']
first_day_current_month=today.replace(day=1)
last_day_previous_month=(first_day_current_month + relativedelta(days=-1))
print('Last day previous month: ', last_day_previous_month)

print('Set Spark configurations')
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

print('Declaring required functions')
#The retrieve_files 
def retrieve_files(path, file_type, list_dates):
    """This is a function that returns a list of S3 objects that: a) are located within the path parameter,
    b) have the same file type as the parameter, and c) are located in the date partitions from the list_dates
    parameter.
    Parameters
    ----------
    path : str
        The S3 path. Retrieved files must be located within this path.
    file_type : str
        The file type. Retrieved files must have this file type.
    list_dates : list
        List of dates. Retrived files must be in a date partition that belongs to this list.
        """
    bucket=path.split('/')[2]
    prefix='/'.join(path.split('/')[3:])
    list_objects=list(s3.Bucket(bucket).objects.all())
    list_objects=[f's3://{bucket}/{i.key}' for i in list_objects if ((i.key.find(prefix)>=0) & any(x in i.key.lower() for x in list_dates) & (i.key.find(file_type)>=0))]
    return list_objects

print('Reading users relevant data')
users_file=retrieve_files(path=f's3://{data_bucket_name}/data/raw/users/', 
                          file_type='parquet', list_dates=[str(last_day_previous_month)])[0]
print('Users file: {}'.format(users_file))
df_users = spark.read.option("header","true").parquet(users_file).select('id', 'created_date', 'fl_sexo_M',
       'fl_ubicacion_centro', 'vl_edad',
       'fl_tarjeta_chip', 'fl_tarjeta_normal',
       'fl_bancarizado', 'nu_bcra_entidades', 'max_situacion',
       'max_dias_atraso', 'fl_opera_banco')

print('Making required transformations to users dataset')
df_users = df_users.select('*',col('id').cast(LongType()).alias('id2'))
df_users = df_users.drop(col('id')).withColumnRenamed('id2', 'id')
df_users = df_users.withColumn('vl_antiguedad_dias', datediff(to_date(lit(str(first_day_current_month)), 'yyyy-MM-dd'),df_users.created_date))
df_users = df_users.select("*",to_date(col('created_date').cast(StringType()), 'yyyy-MM-dd').alias('created_date2'))
df_users = df_users.drop(col('created_date')).withColumnRenamed('created_date2', 'created_date')
del users_file
gc.collect()

print('Filter users by their creation date')
first_date=(first_day_current_month+relativedelta(months=-months)-timedelta(days=1))
print('First date of history: {}'.format(first_date))
df_users2 = df_users.filter(col('created_date') > lit(str(first_date)))
del df_users
gc.collect()

print('Reading churn labels for model')
labels_file=retrieve_files(path=f's3://{data_bucket_name}/labels/', file_type='csv', list_dates=[str(last_day_previous_month)])[0]
print('Labels file: {}'.format(labels_file))

print('Revisar cuáles usuarios son churners, recuperables o activos con la data de inferencias')
df_labels = spark.read.option("header","true").csv(labels_file).select('id', 'churn_label')
df_labels = df_labels.select('*', col('id').cast(LongType()).alias('id2'))
df_labels = df_labels.drop(col('id')).withColumnRenamed('id2', 'id')
del labels_file
gc.collect()

print('Join users dataset with labels')
df_users3 = df_users2.join(df_labels, on=['id'], how = 'inner')
del df_users2, df_labels
gc.collect()

print('Reading the monthly_stage file')
monthly_stage_file=retrieve_files(f's3://{data_bucket_name}/data/monthly_stage/', 'parquet', list_dates=[str(last_day_previous_month)])[0]
print('Monthly stage file: {}'.format(monthly_stage_file))
df_total=spark.read.option("header","true").parquet(monthly_stage_file)
del monthly_stage_file
gc.collect()

# Making required transformations
df_total= df_total.select('*', col('id').cast(LongType()).alias('id2'))
df_total = df_total.drop(col('id')).withColumnRenamed('id2', 'id')
print('Join monthly stage file with users')
df_total2 = df_total.join(df_users3, on=['id'], how = 'inner')
del df_total
gc.collect()

print('Making required transfromations')
print('Variables to aggregate')
sum_variables_events=['nu_bloqueo', 'nu_incidente','nu_inicio']
last_variables_events=['fl_os_android',
                       'fl_carrier_1','fl_carrier_2','fl_carrier_3','fl_carrier_4']

sum_variables_transactions=[i for i in df_total2.columns 
                           if i[:3] in ['vl_','nu_'] and 
                              i not in sum_variables_events+
                                       last_variables_events+
                                      ['vl_edad', 'vl_antiguedad_dias', 'nu_bcra_entidades']] 

print('Sum variables')
df_total2=df_total2.select('*', col('id').cast(LongType()).alias('id2'))
df_total2=df_total2.drop(col('id')).withColumnRenamed('id2', 'id')
sum_variables=sum_variables_transactions+sum_variables_events
sum_exprs = [sum(x).alias('{0}'.format(x)) for x in sum_variables]
df_base=df_total2.groupBy('id').agg(*sum_exprs)
df_base=df_base.select('*', col('id').cast(LongType()).alias('id2'))
df_base=df_base.drop(col('id')).withColumnRenamed('id2', 'id')

print('Take the last value for certain variables')
last_exprs = [last(x).alias('{0}'.format(x)) for x in last_variables_events]
df_base1=df_base.join(df_total2.groupBy('id').agg(*last_exprs),on='id',how='inner')
del df_base, last_exprs, sum_exprs, sum_variables, sum_variables_events, sum_variables_transactions
gc.collect()
df_base2=df_base1.join(df_users3, how='inner', on='id')
del df_users3, df_total2, df_base1
gc.collect()

print('Cast numeric variables')
numeric_columns = [f.name for f in df_base2.schema.fields if (isinstance(f.dataType, (DoubleType, IntegerType, LongType, ShortType, FloatType,DecimalType)))]
numeric_columns=[i for i in numeric_columns if i not in ['id']]
integer_columns=[i for i in numeric_columns if ((i.startswith(('fl_', 'nu_', 'cd_', 'max_')) )| (any(x in i.lower() for x in ['vl_edad','vl_antiguedad_dias'])))]
double_columns=[i for i in numeric_columns if i not in integer_columns]
for i in integer_columns:
    df_base2=df_base2.withColumn(i, col(i).cast(IntegerType()))
for d in double_columns:
    df_base2=df_base2.withColumn(d, col(d).cast(DoubleType()))

print('Write the data in S3 with parquet format')
df_base2.write\
    .format('parquet')\
    .save(f's3://{data_bucket_name}/data/processed/processed_dt={str(last_day_previous_month)}', mode='overwrite')
