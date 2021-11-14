# sm1-DE-assignment-2

## Deployments

### Ingest

To deploy ingest code, you need to first build a Docker image and upload it to `GCR` on Google Cloud using:

```
gcloud builds submit --tag gcr.io/de-assignment-2/ingest-dta ingest/
```

Then, you create a virtual machine on GCP's compute engine service, and it'll ingest all available data automatically. 

### Process

Upload `process/app.py` into a bucket on blob storage, then create a cluster, and assign `app.py` as a `PySpark` job to it. Also note
you need to pass the following `jar` file to Extra Jar files section in the job menu to be able to write into BigQuery:

```
 gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar 
 ```
