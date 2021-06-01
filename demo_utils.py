# to configure spark, follow the demo at https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/master?urlpath=lab/tree/quickstart_demo_deduplications.ipynb

from IPython.display import display, Markdown
from splink.validate import _get_schema

from IPython.display import display, Markdown
from splink.validate import _get_schema

from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType
import pyspark.sql.functions as f


def get_spark():
    conf = SparkConf()

    # Load in a jar that provides extended string comparison functions such as Jaro Winkler.
    # Splink

    # No longer needed in spark 3.0?
    #conf.set("spark.driver.extraClassPath", "C:\\Spark\\spark-3.1.1-bin-hadoop2.7\\jars\\scala-udf-similarity-0.0.7.jar")
    #conf.set("spark.driver.extraClassPath", "C:\\Spark\\spark-3.1.1-bin-hadoop2.7\\jars\\scala-udf-JaroWinkler-0.0.1.jar")

    conf.set("spark.jars", "C:\\Spark\\spark-3.1.1-bin-hadoop2.7\\jars\\scala-udf-similarity-0.0.7.jar")
    conf.set("spark.jars", "C:\\Spark\\spark-3.1.1-bin-hadoop2.7\\jars\\scala-udf-JaroWinkler-0.0.1.jar")
    # SET TO YOUR SPARK INSTALATION

    # WARNING:
    # These config options are appropriate only if you're running Spark locally!!!
    conf.set("spark.driver.memory", "1g")
    conf.set("spark.sql.shuffle.partitions", "4")
    
    #conf.set("spark.sql.files.maxPartitionBytes","536870912")	
    #conf.set("spark.sql.files.maxPartitionBytes","250000000")	
    #conf.set("spark.sql.files.maxPartitionBytes","134217728")	

    sc = SparkContext.getOrCreate(conf=conf)
    sc.setCheckpointDir("temp_graphframes/")
    spark = SparkSession(sc)

    # Register UDFs
    from pyspark.sql import types

    '''
    spark.udf.registerJavaFunction(
        "jaro_winkler_sim",
        "uk.gov.moj.dash.linkage.JaroWinklerSimilarity",
        types.DoubleType(),
    )
    spark.udf.registerJavaFunction(
        "Dmetaphone", "uk.gov.moj.dash.linkage.DoubleMetaphone", types.StringType()
    )
    '''

    return spark


def render_key_as_markdown(key, is_col=False):
    md = []
    schema = _get_schema()
    if is_col:
        value = schema["properties"]["comparison_columns"]["items"]["properties"][key]
    else:
        value = schema["properties"][key]

    if "title" in value:
        md.append(f"**Summary**:\n{value['title']}")

    if "description" in value:

        md.append(f"\n**Description**:\n{value['description']}")

    if "type" in value:
        md.append(f"\n**Data type**: {value['type']}")

    if "enum" in value:

        enum = [f"`{e}`" for e in value["enum"]]
        enum = ", ".join(enum)
        md.append(f"\n**Possible values**: {enum}")

    if "default" in value:
        md.append(f"\n**Default value if not provided**: {value['default']}")

    if "examples" in value:
        if is_col:

            if len(value["examples"]) > 0:
                ex = value["examples"][0]
                if type(ex) == str:
                    ex = f'"{ex}"'

                example = (
                    "```",
                    "settings = {",
                    '    "comparison_columns: [',
                    "    {",
                    f'        "{key}": {ex}',
                    "    }",
                    "]",
                    "```",
                )
                example = "\n".join(example)
                md.append("\n**Example**:\n")
                md.append(example)
        else:
            if len(value["examples"]) > 0:
                ex = value["examples"][0]
                if type(ex) == str:
                    ex = f'"{ex}"'
                example = ("```", "settings = {", f'    "{key}": {ex}', "}", "```")
                example = "\n".join(example)
                md.append("\n**Example**:\n")
                md.append(example)

    return Markdown("\n".join(md))
