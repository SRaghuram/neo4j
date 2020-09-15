# Benchmarking batch infrastructure deployment

In order to deploy benchmarking infrastructure you will need Cloud Deployment Kit CLI installed, 
follow these [instructions](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html),

Once you are set up, run this command

    cdk deploy -c ami-name="benchmarking 1598349694" -c stage="staging" -c batch-infrastructure="aws-batch-infra.json"
    
where:

* `ami-name`, is AMI name of image you want to use,
* `batch-stack`, is name of your batch stack, this is optional, default is `benchmarking`
* `stage', is your batch stack stage, i.e. `production` or staging
* `batch-infrastructure`, is a file which describes your deployment, you can find example file in `aws-batch-infra.json`