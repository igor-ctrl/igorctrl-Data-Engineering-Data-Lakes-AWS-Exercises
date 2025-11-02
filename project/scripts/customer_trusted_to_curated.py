import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node CustomerTrustedNode
CustomerTrustedNode_node1762049248719 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrustedNode_node1762049248719")

# Script generated for node AccelerometerTrustedNode
AccelerometerTrustedNode_node1762049277331 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedNode_node1762049277331")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT
      customer.customerName,
      customer.email,
      customer.phone,
      customer.birthDay,
      customer.serialNumber,
      customer.registrationDate,
      customer.lastUpdateDate,
      customer.shareWithResearchAsOfDate,
      customer.shareWithPublicAsOfDate,
      customer.shareWithFriendsAsOfDate
  FROM myDataSource customer
  INNER JOIN myDataSource1 accel
  ON customer.email = accel.user
'''
SQLQuery_node1762049296948 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource1":AccelerometerTrustedNode_node1762049277331, "myDataSource":CustomerTrustedNode_node1762049248719}, transformation_ctx = "SQLQuery_node1762049296948")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1762049296948, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1762049486913", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1762049516729 = glueContext.getSink(path="s3://stedi-lake-house-igor-tch/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1762049516729")
AmazonS3_node1762049516729.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
AmazonS3_node1762049516729.setFormat("json")
AmazonS3_node1762049516729.writeFrame(SQLQuery_node1762049296948)
job.commit()