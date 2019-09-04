/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class AWSBatchJobScheduler implements JobScheduler
{
    private static final Logger LOG = LoggerFactory.getLogger( AWSBatchJobScheduler.class );

    public static JobScheduler create( String region, String awsKey, String awsSecret, String jobQueue, String jobDefinition )
    {
        Objects.requireNonNull( awsKey );
        Objects.requireNonNull( awsSecret );
        BasicAWSCredentials credentials = new BasicAWSCredentials( awsKey, awsSecret );
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider( credentials );
        return new AWSBatchJobScheduler( AWSBatchClientBuilder.standard()
                                                              .withCredentials( credentialsProvider )
                                                              .withRegion( region )
                                                              .build(),
                                         getJobQueueCustomName( jobQueue, credentials, region ),
                                         jobDefinition );
    }

    private final AWSBatch awsBatch;
    private final String jobDefinition;
    private final String jobQueue;

    public AWSBatchJobScheduler( AWSBatch awsBatch, String jobQueue, String jobDefinition )
    {
        this.awsBatch = awsBatch;
        this.jobQueue = jobQueue;
        this.jobDefinition = jobDefinition;
    }

    private static String getJobQueueCustomName( String jobQueue, AWSCredentials credentialsProvider, String region )
    {
        AmazonCloudFormationClientBuilder amazonCloudFormationClientBuilder = AmazonCloudFormationClientBuilder.standard();
        AmazonCloudFormation amazonCloudFormation = amazonCloudFormationClientBuilder
                .withCredentials( new AWSStaticCredentialsProvider( credentialsProvider ) )
                .withRegion( region )
                .build();
        return amazonCloudFormation.describeStacks( new DescribeStacksRequest().withStackName( "benchmarking" ) ).getStacks().stream().flatMap(
                stack -> stack.getOutputs().stream() ).filter( output -> output.getOutputKey().equals( jobQueue ) ).map(
                Output::getOutputValue ).findFirst().get();
    }

    @Override
    public JobId schedule( URI workerArtifactUri, InfraParams infraParams, RunWorkloadParams runWorkloadParams )
    {
        Map<String,String> paramsMap = new HashMap<>();
        paramsMap.putAll( infraParams.asMap() );
        paramsMap.putAll( runWorkloadParams.asMap() );
        // not a common infra command arg, but required by bootstrap-worker.sh to retrieve jar and launch run-worker command
        paramsMap.put( InfraParams.CMD_WORKER_ARTIFACT_URI, workerArtifactUri.toString() );

        String jobName = getJobName( runWorkloadParams.workloadName() );
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
        List<JobStatus> jobsStatuses = awsBatch.describeJobs( new DescribeJobsRequest().withJobs( jobIdsAsStrings ) )
                                               .getJobs()
                                               .stream()
                                               .map( AWSBatchJobScheduler::jobStatus )
                                               .collect( Collectors.toList() );
        LOG.info( "current jobs statuses:\n{}", jobsStatuses.stream().map( Object::toString ).collect( joining( "\n" ) ) );
        return jobsStatuses;
    }

    private static String getJobName( String workload )
    {
        return String.format( "macro-%s", workload );
    }

    private static JobStatus jobStatus( JobDetail jobDetail )
    {
        return new JobStatus( jobDetail.getJobId(), jobDetail.getStatus() );
    }
}
