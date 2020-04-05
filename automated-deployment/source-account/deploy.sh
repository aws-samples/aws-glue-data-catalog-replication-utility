#!/bin/bash
sflag=false
nflag=false
pflag=false

DIRNAME=$(dirname "$0")

usage () { echo "
    -h -- Opens up this help message
    -a -- Target AWS Account ID
    -n -- Name of the CloudFormation stack
    -p -- Name of the AWS profile to use
    -s -- Name of S3 bucket to upload artifacts to
"; }
options=':a:n:p:s:h'
while getopts $options option
do
    case "$option" in
        a  ) aflag=true; TARGET_ACCOUNT=$OPTARG;;
        n  ) nflag=true; STACK_NAME=$OPTARG;;
        p  ) pflag=true; PROFILE=$OPTARG;;
        s  ) sflag=true; S3_BUCKET=$OPTARG;;
        h  ) usage; exit;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done

if ! $aflag
then
    echo "-a not specified, the target AWS account ID (12 digits) must be specified. Aborting..." >&2
    exit 0
fi
if ! $pflag
then
    echo "-p not specified, using default..." >&2
    PROFILE="default"
    SOURCE_REGION=$(aws configure get region --profile ${PROFILE})
    SOURCE_ACCOUNT=$(aws sts get-caller-identity --profile ${PROFILE} | python3 -c "import sys, json; print(json.load(sys.stdin)['Account'])")
fi
if ! $sflag
then
    S3_BUCKET=glue-data-catalog-replication-$SOURCE_REGION-$SOURCE_ACCOUNT
fi
if ! $nflag
then
    STACK_NAME="glue-data-catalog-replication-source"
fi

echo "Checking if bucket exists ..."
if ! aws s3 ls $S3_BUCKET --profile $PROFILE; then
  echo "S3 bucket named $S3_BUCKET does not exist. Create? [Y/N]"
  read choice
  if [ $choice == "Y" ] || [ $choice == "y" ]; then
    aws s3 mb s3://$S3_BUCKET --profile $PROFILE
  else
    echo "Bucket does not exist. Deploy aborted."
    exit 1
  fi
fi

mkdir $DIRNAME/output
aws cloudformation package --profile $PROFILE --template-file $DIRNAME/template.yaml --s3-bucket $S3_BUCKET --output-template-file $DIRNAME/output/packaged-template.yaml

echo "Checking if stack exists ..."
if ! aws cloudformation describe-stacks --profile $PROFILE --stack-name $STACK_NAME; then
  echo -e "Stack does not exist, creating ..."
  aws cloudformation create-stack \
    --stack-name $STACK_NAME \
    --parameters file://$DIRNAME/parameters.json \
    --template-body file://$DIRNAME/output/packaged-template.yaml \
    --tags file://$DIRNAME/tags.json \
    --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" \
    --profile $PROFILE

  echo "Waiting for stack to be created ..."
  aws cloudformation wait stack-create-complete --profile $PROFILE \
    --stack-name $STACK_NAME
else
  echo -e "Stack exists, attempting update ..."

  set +e
  update_output=$( aws cloudformation update-stack \
    --profile $PROFILE \
    --stack-name $STACK_NAME \
    --parameters file://$DIRNAME/parameters.json \
    --template-body file://$DIRNAME/output/packaged-template.yaml \
    --tags file://$DIRNAME/tags.json \
    --capabilities "CAPABILITY_NAMED_IAM" "CAPABILITY_AUTO_EXPAND" 2>&1)
  status=$?
  set -e

  echo "$update_output"

  if [ $status -ne 0 ] ; then
    # Don't fail for no-op update
    if [[ $update_output == *"ValidationError"* && $update_output == *"No updates"* ]] ; then
      echo -e "\nFinished create/update - no updates to be performed";
      exit 0;
    else
      exit $status
    fi
  fi

  echo "Waiting for stack update to complete ..."
  aws cloudformation wait stack-update-complete --profile $PROFILE \
    --stack-name $STACK_NAME 
  echo "Finished create/update successfully!"
fi

echo "Subscribing Target account to SNS Schema Distribution topic..."
aws sns add-permission --label lambda-access --aws-account-id $TARGET_ACCOUNT \
--topic-arn arn:aws:sns:$SOURCE_REGION:$SOURCE_ACCOUNT:SchemaDistributionSNSTopic \
--action-name Subscribe ListSubscriptionsByTopic Receive