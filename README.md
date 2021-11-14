# sm1-DE-assignment-2

## Deployments

### Ingest

To deploy ingest code, you need to first build a Docker image and upload it to `GCR` on Google Cloud using:

```
gcloud builds submit --tag gcr.io/de-assignment-2/ingest-dta ingest/
```

Then, you create a virtual machine on GCP's compute engine service, and it'll ingest all available data automatically. 
