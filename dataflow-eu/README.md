# EU Dataflow
This module holds several classes that do oneoff-type jobs via dataflow. The majority of classes 
are responsible for loading data into bigtable in various capacities. 

## PiiExpansion Loader

Details of this job can be found [here](../docs/bigtable_schema.md)

### How to run Staging
```
mvn compile exec:java -Dexec.mainClass=com.liveramp.international.bigtable.PiiExpansionDataLoadBigtable -Dexec.args="--runner=DataflowRunner --project=international-gramercy-staging --region=europe-west1 --serviceAccount=svc-tf-dataflow-gramercy@international-gramercy-staging.iam.gserviceaccount.com --workerMachineType=n1-highmem-96 --zone=europe-west1-b --workerDiskType=compute.googleapis.com/projects/international-gramercy-staging/europe-west1-b/diskTypes/pd-ssd \
--jobName=<JOBNAME> \
--subnetwork=https://www.googleapis.com/compute/v1/projects/international-gramercy-staging/regions/europe-west1/subnetworks/international-gramercy-staging-europe-west1-01 \
--emailInputFile=<INPUT FILE FOR EMAIL> \
--phoneInputFile=<INPUT FILE FOR PHONE> \
--projectId=international-gramercy-staging \
--bigTableInstanceId=com-liveramp-bigtable \
--tableId=pii_expansion \
--lrRegion=<REGION>\
```

### How to run Prod

```
mvn compile exec:java -Dexec.mainClass=com.liveramp.international.bigtable.PiiExpansionDataLoadBigtable -Dexec.args="--runner=DataflowRunner --project=international-gramercy --region=europe-west1 --serviceAccount=svc-tf-dataflow-gramercy@international-gramercy.iam.gserviceaccount.com --workerMachineType=n1-highmem-96 --zone=europe-west1-b --workerDiskType=compute.googleapis.com/projects/international-gramercy/europe-west1-b/diskTypes/pd-ssd \
--jobName=<JOBNAME> \
--subnetwork=https://www.googleapis.com/compute/v1/projects/international-gramercy/regions/europe-west1/subnetworks/international-gramercy-europe-west1-01 \
--emailInputFile=<INPUT EMAIL FILE LOCATION> \
--phoneInputFile=<INPUT PHONE FILE LOCATION>  \
--projectId=international-gramercy \
--bigTableInstanceId=com-liveramp-bigtable \
--tableId=pii_expansion \
--lrRegion=<REGION> \
--maxNumWorkers=20"
```


## ClinkLoader
Code: [ClinkLoader.java](./src/java/com/liveramp/international/bigtable/ClinkLoader.java) 

This class is responsible for loading two-way data into Bigtable. Consider the following example: If the input file is laid out like so, `KEY | VALUE1 | VALUE2 | VALUE3`. This class will load the data into two different bigtable stores. One with "KEY" as the rowkey and the three values as values within the column family. The other will have the three VALUEs as rowkeys and "KEY" as the value in the column family.

It's primarily used for loading the `uk_clink_email`, `uk_email_clink`, `fr_clink_email` and `fr_email_clink` tables.   

#### Running ClinkLoader
This is an example (PROD) command to run the CLinkLoader for UK hashed email data
```
mvn compile exec:java -Dexec.mainClass=com.liveramp.international.bigtable.ClinkLoader -Dexec.args="\
--jobName=load-bigtable-ukrema \
--project=international-gramercy \
--region=europe-west1 \
--subnetwork=https://www.googleapis.com/compute/v1/projects/international-gramercy/regions/europe-west1/subnetworks/international-gramercy-europe-west1-03 \
--serviceAccount=svc-tf-dataflow-gramercy@international-gramercy.iam.gserviceaccount.com \
--runner=DataflowRunner \
--maxNumWorkers=20 \
--projectId=international-gramercy \
--inputFile=gs://com-liveramp-eu-pii-bucket/updates/HASHED_EMAIL_UK_20200220.out \
--bigtableInstanceId=com-liveramp-bigtable \
--clinkAsRowkeyTable=uk_clink_email \
--clinkAsValueTable=uk_email_clink \
--columnFamily=ukrema"
```

## Deploying Audience Key Publishing Changes
For now, deploying changes to the audience key publishing workflow is a manual process.
It involves mostly involves deploying dataflow templates (similar to a JAR) to a gcs bucket.
There are two different templates that need to be deployed -- one for incremental refreshes and one for full refreshes.
The process is the same for both, with different parameter values.

*NOTE* These commands will appear to fail, with Maven spitting out a "Build Failed" error. However, if you scroll up a
bit, you should see something like following:
`16:13:10.060 [com.liveramp.dataflow.akp.AKPFullRefreshWorkflow.main()] INFO org.apache.beam.runners.dataflow.DataflowRunner - Template successfully created.`

#### Full Refresh
```
BUCKET_NAME=dataflow-staging-europe-west1-1013940921430
SUBNET=international-gramercy-europe-west1-01
REGION=europe-west1
PROJECT_ID=international-gramercy
GCS_TEMP_LOCATION=gs://$BUCKET_NAME/akp_templates/full_refresh/temp
GCS_TEMPLATE_LOCATION=gs://$BUCKET_NAME/akp_templates/full_refresh/job_template.json
GCS_STAGING_LOCATION=gs://$BUCKET_NAME/akp_templates/full_refresh/staging
NUM_OF_MAX_WORKERS=10

mvn compile exec:java -Dexec.mainClass=com.liveramp.dataflow.akp.AKPFullRefreshWorkflow -Dexec.args="--runner=DataflowRunner \
                               --project=$PROJECT_ID \
                               --region=$REGION \
                               --stagingLocation=$GCS_STAGING_LOCATION \
                               --templateLocation=$GCS_TEMPLATE_LOCATION \
                               --tempLocation=$GCS_TEMP_LOCATION \
								  --subnetwork=https://www.googleapis.com/compute/v1/projects/$PROJECT_ID/regions/$REGION/subnetworks/$SUBNET \
                               --usePublicIps=false"
```

#### Incremental
```
BUCKET_NAME=dataflow-staging-europe-west1-1013940921430
SUBNET=international-gramercy-europe-west1-01
PROJECT_ID=international-gramercy
REGION=europe-west1
GCS_TEMP_LOCATION=gs://$BUCKET_NAME/akp_templates/incremental/temp
GCS_TEMPLATE_LOCATION=gs://$BUCKET_NAME/akp_templates/incremental/job_template.json
GCS_STAGING_LOCATION=gs://$BUCKET_NAME/akp_templates/incremental/staging
NUM_OF_MAX_WORKERS=10

mvn compile exec:java -Dexec.mainClass=com.liveramp.dataflow.akp.AKPIncrementalWorkflow -Dexec.args="--runner=DataflowRunner \
                               --project=$PROJECT_ID \
                               --region=$REGION \
                               --stagingLocation=$GCS_STAGING_LOCATION \
                               --templateLocation=$GCS_TEMPLATE_LOCATION \
                               --tempLocation=$GCS_TEMP_LOCATION \
								  --subnetwork=https://www.googleapis.com/compute/v1/projects/$PROJECT_ID/regions/$REGION/subnetworks/$SUBNET \
                               --usePublicIps=false"
```

### Testing Audience Key Publishing Manually
You can manually kick off an audience key publishing job from your console after creating the template (as described above).
There are required parameters for execution of dataflow jobs in general, and for this specific dataflow job defined in `AkpLoadingOptions`.
These are:
* job name (all df jobs)
* service account email (all df jobs)
* template location (all df jobs)
* zone (all df jobs)

* inputFile (this df job)
* anaId (this df job)
* cidKey (this df job)

#### Launching the job
```
JOB_NAME="my-job"
INPUT_FILE="gs://<path>/<to>/<file.csv>"
ANA=1234
CID_KEY="SomeHeader"
SERVICE_ACCOUNT_EMAIL=svc-tf-dataflow-gramercy@international-gramercy-staging.iam.gserviceaccount.com
ZONE=europe-west1-b

gcloud dataflow jobs run $JOB_NAME \
  --gcs-location=gs://$BUCKET_NAME/incremental_refresh.json \
  --zone=$ZONE \
  --service-account-email=$SERVICE_ACCOUNT_EMAIL \
  --parameters anaId=$ANA,inputFile=$NPUT_FILE,cidKey=$CID_KEY
```
After launching the job, you should be able to track the status in the GCP console.


# Loading ARL<>PEL Data into Bigtable

This is a Maven project with the Apache Beam SDK that will ran by Google Dataflow. The code does following
1. Generate ARL->PEL mapping file from HashedEmail->Clink file
2. Load ARL->PEL mapping file into Bigtable

Please be aware that this need to run in two Google projects due to privacy concern

## Getting Started

Make sure your gcloud is in the correct project that dataflow job run and you are logged in with your liveramp email
```$ gcloud config list
[core]
account = <XXX>@liveramp.com
disable_usage_reporting = True**
project = XXXX
```

### Concept
GCP components involved:
1. GCS: store input file, staging files and template
2. Dataflow: run the code
3. Google KMS:

## Getting Started
Background
Dataflow:
Dataflow template: allow you to stage your pipelines on Cloud Storage and execute them from a variety of environments

## Deployment

### Step1: Create Cloud Dataflow template for generating hashed email->clink

Project ID = international-gramercy-staging


The assumption here is that the hashed email->clink file is located in a GCS bucket.

Set up projects and GCS locations
```
#Project for Dataflow job to run
PROJECT_ID=international-gramercy

#Subnet to run Dataflow job in the project
SUBNET=international-gramercy-europe-west1-01

#Max number of compute instance to spin up during job
NUM_OF_MAX_WORKERS=20

# following bucket must be created beforehand and input file has been uploaded to GCS_INPUT_FILE

#GCS path for Cloud Dataflow to stage your binary files
GCS_STAGING_LOCATION=gs://arl-pel-staging/staging

#GCS path of template file that Maven will create
GCS_TEMPLATE_LOCATION=gs://arl-pel-staging/template

#GCS path for Cloud Dataflow to stage any temporary files.
GCS_TEMP_LOCATION=gs://arl-pel-staging/temp

gcloud config set project $PROJECT_ID
```

Generate template
```
mvn compile exec:java \
-X \
-Dexec.mainClass=com.liveramp.dataflow.arlpel.ConvertRawToArlPel \
-Dexec.args="--runner=DataflowRunner \
--project=$PROJECT_ID \
--stagingLocation=$GCS_STAGING_LOCATION \
--templateLocation=$GCS_TEMPLATE_LOCATION \
--tempLocation=$GCS_TEMP_LOCATION \
--maxNumWorkers=$NUM_OF_MAX_WORKERS \
--subnetwork=https://www.googleapis.com/compute/v1/projects/$PROJECT_ID/regions/europe-west1/subnetworks/$SUBNET \
--usePublicIps=false"
```

(Aniruddh: java.lang.reflect.InvocationTargetException will show but template will be created
as long as you see "Template successfully created." )


### Step2: Execute template from step1
Dataflow job setup

```
JOB_NAME=<a meaning full name for the job>
ZONE=europe-west1-b
DATAFLOW_SVC=svc-dataflow-gramercy@international-gramercy.iam.gserviceaccount.com

#GCS path for input HashedEmail->Clink
GCS_INPUT_FILE=gs://com-liveramp-eu-pii-bucket/Hashed_Emails_UK_20180627195902.out.gz

#GCS path for Dataflow job output file
GCS_OUTPUT_FILE=gs://eu-central-prod-anonymous-mappings/bigtable
```

```
gcloud dataflow jobs run $JOB_NAME --gcs-location=$GCS_TEMPLATE_LOCATION --zone=$ZONE --service-account-email=$DATAFLOW_SVC --parameters inputFile=$GCS_INPUT_FILE,outputFile=$GCS_OUTPUT_FILE
```

It takes about 40min for the job to finish. Output file will be found in gcs defined in GCS_OUTPUT_FILE

### Step3: Create Cloud Dataflow template for loading ARL->PEL to big table

Set up projects and GCS locations
```
#Project for Dataflow job to run
PROJECT_ID=eu-central-prod

#Subnet to run Dataflow job in the project
SUBNET=eu-central-prod-dataflow-1

#Max number of compute instance to spin up during job
NUM_OF_MAX_WORKERS=20

# following bucket must be created beforehand and input file has been uploaded to GCS_INPUT_FILE

#GCS path for Cloud Dataflow to stage your binary files
GCS_STAGING_LOCATION=gs://eu-central-prod-dataflow-staging/staging

#GCS path of template file that Maven will create
GCS_TEMPLATE_LOCATION=gs://eu-central-prod-dataflow-staging/template

#GCS path for Cloud Dataflow to stage any temporary files.
GCS_TEMP_LOCATION=gs://eu-central-prod-dataflow-staging/temp

gcloud config set project $PROJECT_ID
```

Generate template
```
mvn compile exec:java \
-X \
-Dexec.mainClass=com.liveramp.dataflow.arlpel.LoadArlPelBigtable \
-Dexec.args="--runner=DataflowRunner \
--project=$PROJECT_ID \
--stagingLocation=$GCS_STAGING_LOCATION \
--templateLocation=$GCS_TEMPLATE_LOCATION \
--tempLocation=$GCS_TEMP_LOCATION \
--projectId=$PROJECT_ID \
--maxNumWorkers=$NUM_OF_MAX_WORKERS \
--subnetwork=https://www.googleapis.com/compute/v1/projects/$PROJECT_ID/regions/europe-west1/subnetworks/$SUBNET \
--usePublicIps=false"
```

### Step4: Execute template from step3
```
JOB_NAME=<a meaning full name for the job>
ZONE=europe-west1-b

#GCS path for input ARL->PEL file
GCS_INPUT_FILE=gs://eu-central-prod-anonymous-mappings/bigtable-00*

#Bigtable Settings
BIGTABLE_INSTANCE=pixel-serving
BIGTABLE_TABLE=arl_to_pel
BIGTABLE_FAMILY=uk
```

```
gcloud dataflow jobs run $JOB_NAME --gcs-location=$GCS_TEMPLATE_LOCATION --zone=$ZONE --parameters inputFile=$GCS_INPUT_FILE,bigTableInstanceId=$BIGTABLE_INSTANCE,bigTableTableName=$BIGTABLE_TABLE,bigtableColumnFamily=$BIGTABLE_FAMILY
```


## Dataflow Service Account

Currently we are using the default dataflow account.

In future we should create svc in terraform and most restritive roles to
access bigtable, GCS, KMS across projects.


##  Running tests

The tests for loading arl->pel data into Bigtable start up a local Bigtable emulator in a docker container. Please make sure that docker is installed and the daemon is running on the machine running the tests.
