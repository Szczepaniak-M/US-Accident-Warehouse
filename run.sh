# Download data to cluster
BUCKET_ID="example"
hadoop fs -copyToLocal gs://$BUCKET_ID/us-accidents.zip
unzip us-accidents.zip
hadoop fs -mkdir -p input/us-accidents
hadoop fs -copyFromLocal us-accidents/* input/us-accidents

# Create Delta lake tables
spark-submit --class pl.michalsz.spark.CreateTable --master yarn --num-executors 1 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:1.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog us-accidents-warehouse_2.12-1.0.0.jar

# Fill Surrounding table
spark-submit --class pl.michalsz.spark.SurroundingLoader --master yarn --num-executors 1 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:1.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog us-accidents-warehouse_2.12-1.0.0.jar

# Fill Temperature table
spark-submit --class pl.michalsz.spark.TemperatureLoader --master yarn --num-executors 1 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:1.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog us-accidents-warehouse_2.12-1.0.0.jar

# Fill Visibility table
spark-submit --class pl.michalsz.spark.VisibilityLoader --master yarn --num-executors 1 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:1.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog us-accidents-warehouse_2.12-1.0.0.jar

# Fill WeatherCondition table
spark-submit --class pl.michalsz.spark.WeatherConditionLoader --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 4 --packages io.delta:delta-core_2.12:1.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog us-accidents-warehouse_2.12-1.0.0.jar input/us-accidents

# Fill Time table
spark-submit --class pl.michalsz.spark.TimeLoader --master yarn --num-executors 4 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:1.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog us-accidents-warehouse_2.12-1.0.0.jar input/us-accidents

#  Fill Location table
spark-submit --class pl.michalsz.spark.LocationLoader --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 4 --packages io.delta:delta-core_2.12:1.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog us-accidents-warehouse_2.12-1.0.0.jar input/us-accidents

# Fill Accident table
spark-submit --class pl.michalsz.spark.AccidentLoader --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 4 --packages com.swoop:spark-alchemy_2.12:1.1.0,io.delta:delta-core_2.12:1.0.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog us-accidents-warehouse_2.12-1.0.0.jar input/us-accidents
