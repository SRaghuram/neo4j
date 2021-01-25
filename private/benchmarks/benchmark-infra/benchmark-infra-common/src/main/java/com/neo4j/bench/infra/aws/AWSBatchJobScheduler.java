/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.DescribeJobsRequest;
import com.amazonaws.services.batch.model.JobDetail;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams;
import com.neo4j.bench.infra.AWSCredentials;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobId;
import com.neo4j.bench.infra.JobScheduler;
import com.neo4j.bench.infra.JobStatus;
import com.neo4j.bench.infra.resources.Infrastructure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

public class AWSBatchJobScheduler implements JobScheduler
{

    private static final Logger LOG = LoggerFactory.getLogger( AWSBatchJobScheduler.class );

    /**
     * Create AWS batch job scheduler using default credentials chain, https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html.
     *
     * @param awsCredentials AWS credentials
     * @param infrastructure AWS Batch infrastructure
     * @param batchStack     CloudFormation stack name
     * @return AWS job scheduler
     */
    public static JobScheduler getJobScheduler( AWSCredentials awsCredentials, Infrastructure infrastructure, String batchStack )
    {
        Objects.requireNonNull( awsCredentials );
        Objects.requireNonNull( infrastructure );
        Objects.requireNonNull( batchStack );

        return new AWSBatchJobScheduler( AWSBatchClientBuilder.standard()
                                                              .withCredentials( awsCredentials.awsCredentialsProvider() )
                                                              .withRegion( awsCredentials.awsRegion() )
                                                              .build(),
                                         infrastructure.jobQueue(),
                                         infrastructure.jobDefinition() );
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
    public JobId schedule( URI workerArtifactUri, URI baseArtifactUri, String jobName )
    {
        return schedule( workerArtifactUri, baseArtifactUri, jobName, Collections.emptyMap(), Optional.empty() );
    }

    @Override
    public JobId schedule( URI workerArtifactUri,
                           URI baseArtifactUri,
                           String jobName,
                           String jobParameters,
                           Optional<JobRequestConsumer> jobRequestConsumer )
    {
        return schedule(
                workerArtifactUri,
                baseArtifactUri,
                jobName,
                singletonMap( RunMacroWorkloadParams.CMD_JOB_PARAMETERS, jobParameters ),
                jobRequestConsumer
        );
    }

    private JobId schedule(
            URI workerArtifactUri,
            URI baseArtifactUri,
            String jobName,
            Map<String,String> additionalParameters,
            Optional<JobRequestConsumer> transformer
    )
    {
        LOG.info( "scheduling batch job with worker artifact URI {} and base artifact URI {} and additional parameters {}",
                  workerArtifactUri,
                  baseArtifactUri,
                  additionalParameters );

        assertJobName( jobName );
        Map<String,String> paramsMap = new HashMap<>();
        paramsMap.put( InfraParams.CMD_ARTIFACT_WORKER_URI, workerArtifactUri.toString() );
        paramsMap.put( InfraParams.CMD_ARTIFACT_BASE_URI, baseArtifactUri.toString() );
        paramsMap.putAll( additionalParameters );

        SubmitJobRequest submitJobRequest = new SubmitJobRequest()
                .withJobDefinition( jobDefinition )
                .withJobQueue( jobQueue )
                .withJobName( jobName )
                .withParameters( paramsMap );

        transformer.ifPresent( jobRequestConsumer -> jobRequestConsumer.accept( submitJobRequest ) );

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

    private static JobStatus jobStatus( JobDetail jobDetail )
    {
        return new JobStatus( new JobId( jobDetail.getJobId() ),
                              jobDetail.getStatus(),
                              jobDetail.getContainer().getLogStreamName(),
                              jobDetail.getStatusReason() );
    }

    private static void assertJobName( String jobName )
    {
        Pattern jobNamePattern = Pattern.compile( "\\p{Alnum}(\\p{Alnum}|_|-){0,127}" );
        if ( !jobNamePattern.matcher( jobName ).matches() )
        {
            throw new IllegalArgumentException(
                    format( "job name '%s' should follow these restrictions, https://docs.aws.amazon.com/cli/latest/reference/batch/submit-job.html, " +
                            "the first character must be alphanumeric, and up to 128 letters (uppercase and lowercase), " +
                            "numbers, hyphens, and underscores are allowed",
                            jobName ) );
        }
    }
}
