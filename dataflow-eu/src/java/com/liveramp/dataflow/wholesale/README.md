# Wholesale Workflow

Wholesale is a monthly file generating worklow for FR and UK. See more details [here](https://liveramp.atlassian.net/wiki/spaces/CI/pages/98097096/EU+PO+EU+Linkage+Services+FKA+EU+Wholesale).
This workflow will be done collaboratively by POs and OC engineers

The output of Wholesalfe Dataflow job is a PSV with a fixed header of following irregardless of input delimiter . Eng should inform PO whenenver there is output file
format change since they are doing some calculations. 
```
cookie|md5|sha1|sha256
```
It is possible that column 2 to 4 to contain empty string


## Test and Development
TBD

## Load FR Mapping File to Bigtable 
Who: Eng  
When: First Time Setup Only!   
How:  
Assumptions about FR mapping file (usually stores in *gs://com-liveramp-eu-pii-bucket/stores*)  
    * From given record clink is assumed to be the first column in the file.  
    * From given record only one hashed email will be considered per hash leaving other behind. E.g 
        ```Clink | sha1email1 | sha1email2 |sha256email1 | md5Email1 | sha256Email2
        The record will be considered for following hashes:
        Clink | sha1email1 | sha256email1 | md5Email1 
        ```
    * From given record only first appearing hash will considered from given emails. E.g. 
        ```
        Clink | sha1email1 | sha1email2
        The record considered will be:
        Clink | sha1email1
        ```
    * Email hash type is determined based on the length of the value which is:
        ```
        sha256 - > 64 length
        sha1 -> 40 length
        md5 -> 32 length
        ```


Steps  
1. Locate the file containing clinks and hashed emails into gcs location which need to be loaded into Bigtable.
2. Creating data flow template(Not need if there is no code changes):  
    ```
    mvn compile exec:java \
         -Dexec.mainClass=com.liveramp.international.wholesale.FRWholesaleLoader \
         -Dexec.args="--runner=DataflowRunner \
                      --project=international-gramercy \
                      --stagingLocation=gs://arl-pel-staging/wholesale/staging \
                      --subnetwork=https://www.googleapis.com/compute/v1/projects/international-gramercy/regions/europe-west1/subnetworks/international-gramercy-europe-west1-03 \
                      --templateLocation=gs://arl-pel-staging/wholesale/fr_clink_loader_template --region=europe-west1"
    ```
3. Template is created at location gs://arl-pel-staging/wholesale/fr_clink_loader_template
4. Run Dataflow job that will load the Pels into Bigtable uk_wholesale:
    ```
    gcloud config set project international-gramercy
    export ZONE=europe-west1-b
    export DATAFLOW_SVC=svc-tf-dataflow-gramercy@international-gramercy.iam.gserviceaccount.com
    gcloud dataflow jobs run "fr-wholesale-clinkloader-YYYYDDMM"  --region=europe-west1 --gcs-location=gs://arl-pel-staging/wholesale/fr_clink_loader_template --zone=$ZONE --service-account-email=$DATAFLOW_SVC --parameters sha256Inputpath=<FilePathToBeLoadedIntoBigTable>
    ```
## Running Wholesale

### Wholesale Eng Part
Who executes it: Eng  
When: Monthly upon request by PO or OC ticket
What:
Trigger Dataflow job defined in this folder with monthly files provided. Trigger from local machine.



### Generate Wholesale Dataflow template 
Same template for both UK FR, generate only when code changes!  
```
cd  ~/code/eu_ingestion/dataflow-eu
export GCS_TEMPLATE_LOCATION=gs://arl-pel-staging/wholesale/template
mvn compile exec:java \
     -Dexec.mainClass=com.liveramp.dataflow.wholesale.WholeSaleMatchWorkflow \
     -Dexec.args="--runner=DataflowRunner \
                  --project=international-gramercy \
                  --stagingLocation=gs://arl-pel-staging/wholesale/staging \
                  --subnetwork=https://www.googleapis.com/compute/v1/projects/international-gramercy/regions/europe-west1/subnetworks/international-gramercy-europe-west1-03 \
                  --templateLocation=$GCS_TEMPLATE_LOCATION --region=europe-west1"
```
### Kick Off Dataflow Job

1. Setting common variables for both UK and FR
    ```
    gcloud config set project international-gramercy
    export GCS_TEMPLATE_LOCATION=gs://arl-pel-staging/wholesale/template
    export ZONE=europe-west1-b
    export DATAFLOW_SVC=svc-tf-dataflow-gramercy@international-gramercy.iam.gserviceaccount.com
    ```

2. Country/InputFile specific setting  
    UK
    ```
    COUNTRY=UK
    # Providede by PO
    INPUT_FILE=gs://com-liveramp-eu-wholesale-delivery-input/{Company}/{Company}_EU_$COUNTRY_Match_Data_YYYYMMDD.csv
    # Set to same date as input file
    YYYYMMDD=
   
    #Company should be the name btw gs://com-liveramp-eu-wholesale-delivery-input-prod and file name
    COMPANY=<CHECK_INPUT_FILE> 
    OUTPUT_FILE=gs://com-liveramp-eu-wholesale-delivery-output-prod/"$COMPANY"/"$COMPANY"_LINKAGES_"$COUNTRY"_"$YYYYMMDD".psv
    ```

    FR
    ```
    COUNTRY=FR
    # Providede by PO
    INPUT_FILE=gs://com-liveramp-eu-wholesale-delivery-input/{Company}/{Company}_EU_$COUNTRY_Match_Data_YYYYMMDD.csv
    # Set to same date as input file
    YYYYMMDD=
    #Company should be the name btw gs://com-liveramp-eu-wholesale-delivery-input-prod and file name
    COMPANY=<CHECK_INPUT_FILE> 
    OUTPUT_FILE=gs://com-liveramp-eu-wholesale-delivery-output-prod/"$COMPANY"/"$COMPANY"_LINKAGES_"$COUNTRY"_"$YYYYMMDD".psv
    ```

3. Delimiter setting  
    ```
    For PSV input file or Default
    DELIMITER=\|
    For csv input file
    DELIMITER=,
    ```  

4. Kick off Dataflow Job  
    ```
   JOB_NAME=$COMPANY-wholesale-$COUNTRY-$YYYYMMDD
   gcloud dataflow jobs run $JOB_NAME  --gcs-location $GCS_TEMPLATE_LOCATION --worker-zone $ZONE --service-account-email $DATAFLOW_SVC  --region europe-west1 --max-workers=10 --parameters=^#^inputFile=$INPUT_FILE#outputFile=$OUTPUT_FILE#delimiter="$DELIMITER"
   ```

### Wholesale PO Part
Who executes it: PO  
When: Monthly  
What:  
Extract 90 days of PELs from BigQuery, then a PO will import these PELs into an audience, and then finally they ?deliver? these to a destination which will give the PO these same PELs tied to the partner?s cookie
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

Instructions for PO:  
1. Run [this Bigtable query](https://console.cloud.google.com/bigquery?sq=467137229199:986ada3546b044e2837cfe7eb0a38bf0), replacing country_code with either FR or GB 
2. Export the results to a GCS bucket  
    1. After the query completes, a results pane will appear below. Click the ?Job information? tab  
    2. Click the ?Temporary table? link  
    3. A new pane will open. Click ?Export? on the top right of that pane, and then ?export to GCS?  
    4. Write the results to gs://com-liveramp-eu-bq-exports . Often the results exceed BQ?s single file limit, so you have to put a wildcard path in, e.g. gs://com-liveramp-eu-bq-exports/<MONTH>-90-day-wholesale-FR/*

