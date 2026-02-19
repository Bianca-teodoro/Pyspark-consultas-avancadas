import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

df_video = spark.read.parquet('/content/videos-preparados.snappy (1).parquet', header=True, inferSchema=True, encoding='utf-8')

df_comments = spark.read.parquet('/content/videos-comments-tratados.snappy (1).parquet', header=True, inferSchema=True, encoding='utf-8')

df_video.createOrReplaceTempView('video')
df_comments.createOrReplaceTempView('comments')

df_video.show()
df_comments.show()

join_video_comments = spark.sql("""
        SELECT video.`Video ID`
        FROM video
        JOIN comments ON video.`Video ID` = comments.`Video ID`
""")
join_video_comments.show()

# Criação da repartição
df_video_partition = spark.read.parquet('/content/videos-preparados.snappy (1).parquet')
df_video_partition = df_video_partition.repartition(8)
df_comments_partition = spark.read.parquet('/content/videos-comments-tratados.snappy (1).parquet')
df_comments_partition = df_comments_partition.repartition(8)

df_video_partition.createOrReplaceTempView('tabela_video_temporaria')
df_comments_partition.createOrReplaceTempView('tabela_comments_temporaria')

join_video_comments_partition = spark.sql("""
        SELECT tabela_video_temporaria.`Video ID`
        FROM tabela_video_temporaria
        JOIN tabela_comments_temporaria ON tabela_video_temporaria.`Video ID` = tabela_comments_temporaria.`Video ID`
""")
join_video_comments_partition.show()

#Criação coalesce
df_video_coalesce = spark.read.parquet('/content/videos-preparados.snappy (1).parquet')
df_video_coalesce = df_video.coalesce(4)
df_comments_coalesce = spark.read.parquet('/content/videos-comments-tratados.snappy (1).parquet')
df_comments_coalesce = df_comments.coalesce(4)

df_video_coalesce.createOrReplaceTempView('tabela_video_coalesce_temporaria')
df_comments_coalesce.createOrReplaceTempView('tabela_comments_coalesce_temporaria')

join_video_comments_coalesce = spark.sql("""
        SELECT tabela_video_coalesce_temporaria.`Video ID`
        FROM tabela_video_coalesce_temporaria
        JOIN tabela_comments_coalesce_temporaria ON tabela_video_coalesce_temporaria.`Video ID` = tabela_comments_coalesce_temporaria.`Video ID`
""")
join_video_comments_coalesce.show()

join_video_comments_partition.explain() # No primeiro plano, aparecem operadores Exchange RoundRobinPartitioning(8) — isso mostra que foi usado repartition(n), criando 8 partições distribuídas "em rodízio" (quase igualitário) entre nós do cluster.
join_video_comments_coalesce.explain() #No segundo plano, aparece Coalesce 4 — indicando que cada DataFrame teve suas partições reduzidas para 4 usando .coalesce(4).

#Salvar em .parquet
join_video_comments.write.mode('overwrite').parquet('join_video_comments_parquet')
