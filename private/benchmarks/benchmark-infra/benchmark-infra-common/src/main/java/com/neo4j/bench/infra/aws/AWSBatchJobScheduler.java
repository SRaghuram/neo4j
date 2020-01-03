/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.DescribeJobsRequest;
import com.amazonaws.services.batch.model.JobDetail;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.DescribeStacksRequest;
import com.amazonaws.services.cloudformation.model.Output;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.infra.JobId;
import com.neo4j.bench.infra.JobScheduler;
import com.neo4j.bench.infra.JobStatus;
import com.neo4j.bench.infra.commands.InfraParams;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class AWSBatchJobScheduler implements JobScheduler
{

    /**
     * Create AWS batch job scheduler using default credentials chain, https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html.
     *
     * @param region AWS region
     * @param jobQueue job queue name in CloudFormation stack
     * @param jobDefinition job definition name in CloudFormation stack
     * @param stack CloudFormation stack name
     * @return AWS job scheduler
     */
    public static JobScheduler create(
            String region,
            String jobQueue,
            String jobDefinition,
            String stack )
    {
        AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
        return create( credentialsProvider, region, jobQueue, jobDefinition, stack );
    }

    /**
     * Create AWS batch job scheduler using provided AWS key and secret.
     *
     * @param region AWS region
     * @param awsKey AWS key
     * @param awsSecret AWS secret
     * @param jobQueue job queue name in CloudFormation stack
     * @param jobDefinition job definition name in CloudFormation stack
     * @param stack CloudFormation stack name
     * @return AWS job scheduler
     */
    public static JobScheduler create(
            String region,
            String awsKey,
            String awsSecret,
            String jobQueue,
            String jobDefinition,
            String stack )
    {
        Objects.requireNonNull( awsKey );
        Objects.requireNonNull( awsSecret );

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider( new BasicAWSCredentials( awsKey, awsSecret ) );
        return create( credentialsProvider, region, jobQueue, jobDefinition, stack );
    }

    private static JobScheduler create( AWSCredentialsProvider credentialsProvider,
                                        String region,
                                        String jobQueue,
                                        String jobDefinition,
                                        String stack )
    {
        Objects.requireNonNull( region );
        Objects.requireNonNull( jobQueue );
        Objects.requireNonNull( jobDefinition );
        Objects.requireNonNull( stack );

        return new AWSBatchJobScheduler( AWSBatchClientBuilder.standard()
                                                              .withCredentials( credentialsProvider )
                                                              .withRegion( region )
                                                              .build(),
                                         getJobQueueCustomName( jobQueue, credentialsProvider, region, stack ),
                                         jobDefinition );
    }

    private static String getJobQueueCustomName( String jobQueue, AWSCredentialsProvider credentialsProvider, String region, String stack )
    {
        AmazonCloudFormation amazonCloudFormation = AmazonCloudFormationClientBuilder.standard()
                                                                                     .withCredentials( credentialsProvider )
                                                                                     .withRegion( region )
                                                                                     .build();

        return amazonCloudFormation.describeStacks( new DescribeStacksRequest().withStackName( stack ) )
                                   .getStacks()
                                   .stream()
                                   .flatMap( stacks -> stacks.getOutputs().stream() )
                                   .filter( output -> output.getOutputKey().equals( jobQueue ) )
                                   .map( Output::getOutputValue )
                                   .findFirst()
                                   .orElseThrow( () -> new RuntimeException( format( "job queue %s not found in stack %s ", jobQueue, stack ) ) );
    }

    private final AWSBatch awsBatch;
    private final String jobDefinition;
    private final String jobQueue;

    // package scope for testing only
    AWSBatchJobScheduler( AWSBatch awsBatch, String jobQueue, String jobDefinition )
    {
        this.awsBatch = awsBatch;
        this.jobQueue = jobQueue;
        this.jobDefinition = jobDefinition;
    }

    @Override
    public JobId schedule( URI workerArtifactUri, URI baseArtifactUri, RunWorkloadParams runWorkloadParams )
    {
        Map<String,String> paramsMap = new HashMap<>();
        paramsMap.put( InfraParams.CMD_ARTIFACT_WORKER_URI, workerArtifactUri.toString() );
        paramsMap.put( InfraParams.CMD_ARTIFACT_BASE_URI, baseArtifactUri.toString() );

        String jobName = getJobName( "macro", runWorkloadParams.workloadName(), runWorkloadParams.neo4jVersion().toString(), runWorkloadParams.triggeredBy() );
        SubmitJobRequest submitJobRequest = new SubmitJobRequest()
                .withJobDefinition( jobDefinition )
                .withJobQueue( jobQueue )
                .withJobName( jobName )
                .withParameters( paramsMap );

        SubmitJobResult submitJobResult = awsBatch.submitJob( submitJobRequest );
        return new JobId( submitJobResult.getJobId() );
    }

    @Override
    public List<JobStatus> jobsStatuses( List<JobId> jobIds )
    {
        List<String> jobIdsAsStrings = jobIds.stream().map( JobId::id ).collect( toList() );
        return awsBatch.describeJobs( new DescribeJobsRequest().withJobs( jobIdsAsStrings ) )
                       .getJobs()
                       .stream()
                       .map( AWSBatchJobScheduler::jobStatus )
                       .collect( Collectors.toList() );
    }

    private static String getJobName( String tool, String benchmark, String version, String triggered )
    {
        // job name should follow these restrictions, https://docs.aws.amazon.com/cli/latest/reference/batch/submit-job.html
        // The first character must be alphanumeric, and up to 128 letters (uppercase and lowercase), numbers, hyphens, and underscores are allowed.
        String jobName = format( "%s-%s-%s-%s", tool, benchmark, version, triggered );
        return StringUtils.substring( jobName.replaceAll( "[^\\p{Alnum}|^_|^-]", "_" ), 0, 127 );
    }

    private static JobStatus jobStatus( JobDetail jobDetail )
    {
        return new JobStatus( jobDetail.getJobId(), jobDetail.getStatus(), jobDetail.getContainer().getLogStreamName() );
    }
}
