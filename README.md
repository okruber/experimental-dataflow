# experimental-dataflow

Sandbox repo for running Dataflow tests

**Errors encountered:**  

Deployed Dataflow pipeline error  
"Error occurred in the launcher container: file /opt/dataflow/pipelines/bq_to_gcs/main.py does not exist"  
Solution: dockerignore-file misconfigured

Deployed Dataflow pipeline error  
"Error occurred in the launcher container: Template launch failed. See console logs."  
Solution: Dataflow-runner service account missing correct GCP permissions