import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1733743633661 = glueContext.create_dynamic_frame.from_catalog(database="ingesta", table_name="transacciones_csv", transformation_ctx="AWSGlueDataCatalog_node1733743633661")

# Script generated for node Change Schema
ChangeSchema_node1733743648757 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1733743633661, mappings=[("id", "long", "id", "int"), ("tipo de transaccion", "string", "tipo de transaccion", "string"), ("customer id", "long", "customer id", "int"), ("customer name", "string", "customer name", "string"), ("count pay", "long", "count pay", "long"), ("precio", "long", "precio", "long"), ("tipo de energia", "string", "tipo de energia", "string")], transformation_ctx="ChangeSchema_node1733743648757")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1733743648757, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733743621747", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})

output_path = "s3://electra-data-lake/Bronce/Procesados/Transacciones" 

AmazonS3_node1733743695010 = glueContext.getSink(path=output_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1733743695010")
AmazonS3_node1733743695010.setCatalogInfo(catalogDatabase="procesados",catalogTableName="TransacProcesados")
AmazonS3_node1733743695010.setFormat("glueparquet", compression="gzip")
AmazonS3_node1733743695010.writeFrame(ChangeSchema_node1733743648757)
job.commit()