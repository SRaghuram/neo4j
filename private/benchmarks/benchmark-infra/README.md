# building Docker image

This is a docker image which contains profilers, plus small worker bootstrap
script. This is used in AWS batch job definition in CloudFormation stack down
below.

You can buid it in two simple steps.

  `docker build benchmark-infra-worker -t 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:latest
   docker push 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:latest`

# building AWS Batch AMI

Default AMI used by AWS Batch has limitations on available storage space (10G).
Which is not enough for benchmarking workloads. In order to overcome this limitation we
have to build our own AMI.

There is a requirement that this AMI should be build from
[ECS optimizated AMI](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-optimized_AMI.html)

We are going to use [Packer](https://www.packer.io/) from HashCorp to simplify this process.

In order build new AMI image, you should invoke following command:

  `packer build src/main/ami/benchmark-run-batch-worker/template.json`

This will build new AMI and push it to EC2. Then you will have to use new AMI id
in AWS Batch computing environment configuration.

# deploy AWS Batch stack

AWS Batch is deployed using CloudFormation. Here is easy one liner to deploy/update stack.

  `aws --region eu-north-1 cloudformation deploy --stack-name benchmarking --template-file src/main/stack/aws-batch-formation.json`
