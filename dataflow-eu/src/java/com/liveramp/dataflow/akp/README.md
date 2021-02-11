# AKP Pipelines
The AKP PELs are stored in the Bigtable table arl_to_pel where the row key is a salted hash of the combined identifiers Adnetwork ID and publisher ID. Since all AKP customer data is stored in this one table, there needs to be a way to determine the row keys for a given customer in order to perform a full refresh of their data. For this, we maintain a second Bigtable table arl_diff where the row key is the unhashed version of the identifiers. When a full refresh AKP job runs, it can lookup all the row keys for a given Adnetwork ID in the arl_diff table as all the row keys for a given Adnetwork ID are prefixed by the anaId. The full refresh job can then compute the salted hash of that row key in order to delete the corresponding entry in the arl_to_pel table. 

For both types of AKP pipeline, once any existing data has been deleted (for a full refresh job), the loading process is the same. Namely, to store customer?s PEL data in the arl_to_pel table using the salted hashed row key and the unhashed row keys in the arl_diff table.

When loading AKP data, the column headers in the file containing the Adnetwork ID and customer ID are specified (anaId and cidKey respectively). If the preferredPelKey option is set, this column header is used for the PEL data. Otherwise, the column containing the PELs is determined automatically. 

## There are currently two Dataflow workflows supported in AKP
AKP dataflow jobs are run from templates which are built using Maven. The commands for building the incremental and full refresh templates are executed from the module directory. For example: 

dataflow-eu/

### Common settings
`gcloud config set project eu-central-prod`

### Incremental
1.Template generation

```$xslt
mvn compile exec:java -Dexec.mainClass=com.liveramp.dataflow.akp.AKPIncrementalWorkflow -Dexec.args="\
--runner=DataflowRunner \
--project=eu-central-prod \
--region=europe-west1 \
--stagingLocation=gs://com-liveramp-akp-dataflow-templates/staging \
--templateLocation=gs://com-liveramp-akp-dataflow-templates/incremental/job_template \
--tempLocation=gs://com-liveramp-akp-dataflow-templates/incremental/temp \
--subnetwork=https://www.googleapis.com/compute/v1/projects/eu-central-prod/regions/europe-west1/subnetworks/eu-central-prod-dataflow-1 \
--usePublicIps=false \
--serviceAccount=akp-wholesale@eu-central-prod.iam.gserviceaccount.com"
```

2.DF job kick off 

Provide the details of parameters while launching the job.

Parameters
- PROJECT_ID=<project name where job needs to be run ideally eu-central-prod>
- ANA_ID=<ana id>
- INPUT_FILE=<input file to be processed>
- CID_KEY=<cid key>
- BIGTABLE_INSTANCE=<BIG_TABLE_INSTANCE where tables present example : pixel-serving for eu-central-prod> 
- ARL_DIFF_TABLE=<arl_diff table name example: arl_diff for eu-central-prod>
- ARL_PEL_TABLE=<arl_pel table name example: arl_pel for eu-central-prod>


```$xslt
gcloud dataflow jobs run <incremental-job-name> \
  --region=europe-west1 \
  --gcs-location=gs://com-liveramp-akp-dataflow-templates/incremental/job_template \
  --parameters anaId=$ANA_ID,inputFile=$INPUT_FILE,cidKey=$CID_KEY,bigtableInstance=$BIGTABLE_INSTANCE,arlDiffTable=$ARL_DIFF_TABLE,arlPelTable=$ARL_PEL_TABLE,projectId=$PROJECT_ID
```

### Full refresh
1.Template generation
```$xslt
mvn compile exec:java -Dexec.mainClass=com.liveramp.dataflow.akp.AKPFullRefreshWorkflow -Dexec.args="\
--runner=DataflowRunner \
--project=eu-central-prod \
--region=europe-west1 \
--stagingLocation=gs://com-liveramp-akp-dataflow-templates/staging \
--templateLocation=gs://com-liveramp-akp-dataflow-templates/full_refresh/job_template \
--tempLocation=gs://com-liveramp-akp-dataflow-templates/full_refresh/temp \
--subnetwork=https://www.googleapis.com/compute/v1/projects/eu-central-prod/regions/europe-west1/subnetworks/eu-central-prod-dataflow-1 \
--usePublicIps=false \
--serviceAccount=akp-wholesale@eu-central-prod.iam.gserviceaccount.com"
```

2.DF job kick off

Provide the details of parameters while launching the job.


Parameters
- PROJECT_ID=<project name where job needs to be run ideally eu-central-prod>
- ANA_ID=<ana id>
- INPUT_FILE=<input file to be processed>
- CID_KEY=<cid key>
- BIGTABLE_INSTANCE=<BIG_TABLE_INSTANCE where tables present example : pixel-serving for eu-central-prod>
- ARL_DIFF_TABLE=<arl_diff table name example: arl_diff for eu-central-prod>
- ARL_PEL_TABLE=<arl_pel table name example: arl_pel for eu-central-prod>


```$xslt
gcloud dataflow jobs run <full-refresh-job-name> \
  --region=europe-west1 \
  --gcs-location=gs://com-liveramp-akp-dataflow-templates/full_refresh/job_template \
  --parameters anaId=$ANA_ID,inputFile=$INPUT_FILE,cidKey=$CID_KEY,bigtableInstance=$BIGTABLE_INSTANCE,arlDiffTable=$ARL_DIFF_TABLE,arlPelTable=$ARL_PEL_TABLE,projectId=$PROJECT_ID
```


### Expected data input format:
```$xslt
ISE_HASH=aaa,PEL=bbb
ISE_HASH=ccc,PEL=ddd
```
