setup_gac:
	export GOOGLE_APPLICATION_CREDENTIALS="./.keys/sa.json"

up_localstack:
	docker-compose -f env-deploy/local_stack_compose.yml up

# awslocal s3api list-buckets
 s3awsapi create-bucket --bucket zilqa-analytics