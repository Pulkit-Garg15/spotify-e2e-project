# Databricks notebook source
from jinja2 import Template
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

parameters = [
    {
        "table" : "spotify_cata.silver.factstream",
        "alias" : "factstream",
        "cols" : "factstream.stream_id , factstream.listen_duration"
    },
    {
        "table" : "spotify_cata.silver.dimuser",
        "alias" : "dimuser",
        "cols" : "dimuser.user_id , dimuser.user_name",
        "condition" : "factstream.user_id = dimuser.user_id"
    },
    {
        "table" : "spotify_cata.silver.dimtrack",
        "alias" : "dimtrack",
        "cols" : "dimtrack.track_id , dimtrack.track_name",
        "condition" : "factstream.track_id = dimtrack.track_id"
    }
]

# COMMAND ----------

query_text = """
            SELECT
                {% for param in parameters %}
                    {{ param.cols }}
                        {% if not loop.last %}
                            ,
                        {% endif %}
                {% endfor %}
            FROM
                {% for param in parameters %}
                    {% if loop.first %}
                        {{ param.table }} AS {{ param.alias }}
                    {% else %}
                        LEFT JOIN {{ param.table }} AS {{ param.alias}}
                        ON {{ param.condition }}
                    {% endif %}
                {% endfor %}
"""

# COMMAND ----------

query_template = Template(query_text)
generated_query = query_template.render(parameters = parameters)
print(generated_query)

# COMMAND ----------

df_auto_query_otp = spark.sql(generated_query)
display(df_auto_query_otp)

# COMMAND ----------

window_spec = Window.orderBy(col("listen_duration").desc())
df_auto_query_otp = df_auto_query_otp.withColumn("rn" , row_number().over(window_spec)).filter(col("rn") <= lit(5)).drop("rn")
display(df_auto_query_otp)