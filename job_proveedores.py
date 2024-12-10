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
AWSGlueDataCatalog_node1733668840327 = glueContext.create_dynamic_frame.from_catalog(database="ingesta", table_name="proveedores_csv", transformation_ctx="AWSGlueDataCatalog_node1733668840327")

# Script generated for node Change Schema
ChangeSchema_node1733668865965 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1733668840327, mappings=[("id", "long", "id", "int"), ("nombre de proveedor", "string", "nombre de proveedor", "string"), ("tipo de energia", "string", "tipo de energia", "string"), ("fecha creacion", "string", "fecha creacion", "string"), ("fecha actualizacion", "string", "fecha actualizacion", "string"), ("estado", "long", "estado", "int"), ("desc estado", "string", "desc estado", "string")], transformation_ctx="ChangeSchema_node1733668865965")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1733668865965, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1733665499375", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})

output_path = f"s3://electra-data-lake/Bronce/Procesados/ProveedoresProcesados"

AmazonS3_node1733669146703 = glueContext.getSink(path=output_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1733669146703")



AmazonS3_node1733669146703.setCatalogInfo(catalogDatabase="procesados",catalogTableName="ProveedoresProcesados")
AmazonS3_node1733669146703.setFormat("glueparquet", compression="gzip")
AmazonS3_node1733669146703.writeFrame(ChangeSchema_node1733668865965)
job.commit()