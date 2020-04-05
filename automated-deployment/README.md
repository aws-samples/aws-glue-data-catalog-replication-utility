This guide explains how to deploy the AWS Glue data catalog utility in both the Source and Target AWS accounts.

## Prerequisites:
1. Administrator access to two AWS accounts:
    1. Source AWS account holding the Glue catalog to replicate
    2. Target AWS account where Glue catalog will be replicated
2. The ```aws``` [CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) installed and configured with a profile (```default``` is used if no profile is specified)
3. The ```jq``` [CLI](https://stedolan.github.io/jq/manual/) installed (Test it's present with ```jq --version``` otherwise run ```sudo yum install jq```)

## Deployment

### Source Account:
1. Log in to the Source AWS account console using the Admin role and select the AWS region where the Glue catalog you wish to replicate is located

2. Set up a Cloud9 Environment in the same AWS region (t2.small or higher, Linux AMI) and log into it. While these commands can be run from any machine with a Linux flavored OS, using a Cloud9 environment ensures that required software (e.g. Git) is pre-installed and that neither your computer nor the Cloud9 instance are polluted by existing environment variables

3. Install ```jq```:
    ```bash
    echo y | sudo yum install jq
    ```

4. Git clone this repository:
    ```bash
    git clone https://github.com/aws-samples/aws-glue-data-catalog-replication-utility.git
    cd ./aws-glue-data-catalog-replication-utility/automated-deployment/source-account/
    ```

5. Modify the ```parameters.json``` file with relevant values:
    ```bash
    [
        {
            "ParameterKey": "pDatabasePrefixList", # List of database prefixes separated by a token. E.g. raw_data_,processed_data_. To export all databases, leave as is
            "ParameterValue": ""
        },
        {
            "ParameterKey": "pDatabasePrefixSeparator", # The separator used in the database_prefix_list. E.g. ",". To export all databases, leave as is 
            "ParameterValue": "|"
        },
        {
            "ParameterKey": "pReplicationSchedule", # Cron Expression to schedule and trigger Glue catalog replication. Defaults to everday at midnight and 30 minutes
            "ParameterValue": "cron(30 0 * * ? *)"
        }
    ]
    ```

6. After updating the parameters, run:
    ```bash
    ./deploy.sh -a <TARGET_AWS_ACCOUNT_ID>
    ```
***IMPORTANT***: The ```-a``` parameter is relative to the Target account NOT the Source. If this is the first time you run the script, it will ask to create an S3 bucket to store CloudFormation artificats. Type ```y``` when prompted. Following that, the entire infrastructure required to replicate the Glue catalog from the source account will be deployed

7. Navigate to S3 and locate the ```import-large-table-<randomid>``` bucket. Add a cross-account bucket policy referencing the target account(s). For example:
    ```bash
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::<TARGET_ACCOUNT_ID>:root"
                },
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:ListBucket"
                ],
                "Resource": "arn:aws:s3:::import-large-table-b2465b90-638f-11ea-8000-0a52752701a6"
            }
        ]
    }
    ```
You can add statements for as many target accounts as necessary. More details on how to add a bucket policy can be found here:
	1. https://aws.amazon.com/premiumsupport/knowledge-center/cross-account-access-s3/
	2. https://docs.aws.amazon.com/AmazonS3/latest/dev/example-walkthroughs-managing-access-example2.html

8. This utility replicates your Glue Metadata Catalog. However, access to the ```underlying``` data is still needed if you wish to query it. To achieve that, add a cross-account bucket policy to the bucket holding your data allowing the target account(s) to access it. 

### Target Account:
1. Log in to the Target AWS account console using the Admin role and select the AWS region where you wish to replicate the Glue catalog from the Source account

2. Set up a Cloud9 Environment in the same AWS region (t2.small or higher, Linux AMI) and log into it. While these commands can be run from any machine with a Linux flavored OS, using a Cloud9 environment ensures that required software (e.g. Git) is pre-installed and that neither your computer nor the Cloud9 instance are polluted by existing environment variables

3. Install ```jq```:
    ```bash
    echo y | sudo yum install jq
    ```

4. Git clone this repository:
    ```bash
    git clone https://github.com/aws-samples/aws-glue-data-catalog-replication-utility.git
    cd ./aws-glue-data-catalog-replication-utility/automated-deployment/target-account/
    ```

5. Then run:
    ```bash
    ./deploy.sh -a <SOURCE_AWS_ACCOUNT_ID> -r <SOURCE_AWS_REGION>
    ```
***IMPORTANT***: The ```-a``` and ```-r``` parameters are relative to the Source account NOT the Target. If this is the first time you run the script, it will ask to create an S3 bucket to store CloudFormation artificats. Type ```y``` when prompted. Following that, the entire infrastructure required to replicate the Glue catalog from the source account will be deployed

## Testing the replication:
Back in the Source AWS account in the AWS Lambda console, you can run the GDCReplicationPlanner Lambda function using a Test event to trigger the initial replication