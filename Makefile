setup_gac:
	export GOOGLE_APPLICATION_CREDENTIALS="./.keys/sa.json"

up_localstack:
	docker-compose -f env-deploy/local_stack_compose.yml up

# awslocal s3api list-buckets
 awslocal s3api create-bucket --bucket sample-bucket

 awslocal s3api list-buckets

 export LOCALSTACK_ENDPOINT_URL="http://localhost:4566"

aws configure --profile localstack
# AWS Access Key ID [None]: test
# AWS Secret Access Key [None]: test
# Default region name [None]: us-east-1
# Default output format [None]:


list buckets
aws --endpoint-url=$LOCALSTACK_ENDPOINT_URL s3 ls


