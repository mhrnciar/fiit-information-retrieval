# mhrnciar Freebase Could they meet
spark-submit \
  --class "SparkParser" \
  --master "mesos URL" \
  --files freebase-rdf-latest.gz \
  --conf spark.executor.uri="path to executor" \
  SparkSubmit-1.0.jar freebase-rdf-latest.gz spark-parsed
