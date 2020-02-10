gcloud dataproc jobs submit pyspark wordcount.py \
--cluster=pedro-santos-1203 -- \
gs://la-gcp-labs-resources/data-engineer/dataproc/romeoandjuliet.txt \
gs://pedro-bucket-1/output/