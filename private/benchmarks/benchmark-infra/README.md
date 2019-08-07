# building Docker image

This is a docker image which contains profilers, plus small worker bootstrap
script. This is used in AWS batch job definition in CloudFormation stack down
below.

You can buid it in two simple steps.

	docker build benchmark-infra-worker -t 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:latest
	docker push 065531048259.dkr.ecr.eu-west-1.amazonaws.com/benchmarks-worker:latest

# building AWS Batch AMI

Default AMI used by AWS Batch has limitations on available storage space (10G).
Which is not enough for benchmarking workloads. In order to overcome this limitation we
have to build our own AMI.

There is a requirement that this AMI should be built from
[ECS optimized AMI](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-optimized_AMI.html)

We are going to use [Packer](https://www.packer.io/) from HashCorp to simplify this process.

In order to build a new AMI image, you should invoke the following command:

	packer build src/main/ami/benchmark-run-batch-worker/template.json

This will build new AMI and push it to EC2. Then you will have to use new AMI id
in AWS Batch computing environment configuration.

# deploy AWS Batch stack

AWS Batch is deployed using CloudFormation. Here is easy one liner to deploy/update stack.

	aws --region eu-north-1 cloudformation deploy --stack-name benchmarking --template-file src/main/stack/aws-batch-formation.json

# working locally with worker

The best way to develop and debug worker is to do it through docker container

	docker run -v $HOME/.aws:/root/.aws 535893049302.dkr.ecr.eu-north-1.amazonaws.com/benchmarks-worker:latest /work/bootstrap-worker.sh \

    # *** Schedule/Run Worker ***
    --worker-artifact-uri  \
    s3://benchmarking.neo4j.com/artifacts/benchmark-infra-worker.jar \

    run-worker,
    
    # *** Run Workload ***    
    --workload \
    accesscontrol \
    --db-edition \
    ENTERPRISE \
    --jvm \
    /usr/lib/jvm/java-8-oracle/bin/java \
    --profilers \
    GC \
    --warmup-count \
    1000 \
    --measurement-count \
    1000 \
    --forks \
    1 \
    --time-unit \
    MICROSECONDS \
    --runtime \
    DEFAULT \
    --planner \
    DEFAULT \
    --execution-mode \
    EXECUTE \
    --error-policy \
    REPORT_THEN_FAIL \
    --jvm-args \
    -Xmx4g \
    --neo4j-deployment \
    embedded \
    
    # *** Project Version ***
    --neo4j-commit \
    f3fb07ec92527f740e527e4d128c5c1faf12b8a9 \
    --neo4j-version \
    3.4.14 \
    --neo4j-branch \
    3.4 \
    --neo4j-branch-owner \
    neo4j \
    --tool-commit \
    f3fb07ec92527f740e527e4d128c5c1faf12b8a9 \
    --tool-branch-owner \
    neo4j \
    --tool-branch \
    3.4 \
    --teamcity-build \
    5531608 \
    --parent-teamcity-build \
    5519409 \
    --triggered-by \
    new_infra \

    # *** AWS ***
    --workspace-dir \
    /work/run \
    --aws-secret \
    [aws secret] \
    --aws-key \
    [aws key] \
    --aws-region \
    [aws region] \
    --db-name \
    accesscontrol \

    # *** Benchmark Results Store ***
    --results_store_user \
    client \
    --results_store_pass \
    [result store password] \
    --results_store_uri \
    bolt+routing://e605d648.databases.neo4j.io:7687
