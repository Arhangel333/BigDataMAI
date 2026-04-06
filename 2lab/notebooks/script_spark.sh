spark-submit   --jars /home/jovyan/jars/postgresql-42.6.0.jar   --driver-class-path /home/jovyan/jars/postgresql-42.6.0.jar  /home/jovyan/work/load_csv.py
spark-submit   --jars /home/jovyan/jars/postgresql-42.6.0.jar   --driver-class-path /home/jovyan/jars/postgresql-42.6.0.jar  /home/jovyan/work/star_schema_final_working.py
spark-submit   --jars /home/jovyan/jars/clickhouse-jdbc-0.4.6-shaded.jar,/home/jovyan/jars/postgresql-42.6.0.jar   --driver-class-path /home/jovyan/jars/clickhouse-jdbc-0.4.6-shaded.jar:/home/jovyan/jars/postgresql-42.6.0.jar /home/jovyan/work/reports_to_clickhouse.py
spark-submit \
  --jars /home/jovyan/jars/postgresql-42.6.0.jar \
  --driver-class-path /home/jovyan/jars/postgresql-42.6.0.jar \
  --conf "spark.sql.legacy.allowUntypedScalaUDF=true" \
  /home/jovyan/work/reports_to_mongodb.py

spark-submit \
  --jars /home/jovyan/jars/postgresql-42.6.0.jar \
  --driver-class-path /home/jovyan/jars/postgresql-42.6.0.jar \
  /home/jovyan/work/reports_to_neo4j.py

spark-submit \
  --jars /home/jovyan/jars/postgresql-42.6.0.jar \
  --driver-class-path /home/jovyan/jars/postgresql-42.6.0.jar \
  /home/jovyan/work/reports_to_valkey.py
spark-submit \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  --jars /home/jovyan/jars/postgresql-42.6.0.jar \
  --driver-class-path /home/jovyan/jars/postgresql-42.6.0.jar \
  /home/jovyan/work/reports_to_cassandra.py