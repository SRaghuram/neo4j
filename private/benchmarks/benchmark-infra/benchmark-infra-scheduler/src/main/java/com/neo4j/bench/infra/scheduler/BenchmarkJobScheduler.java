/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.neo4j.bench.infra.AWSCredentials;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobId;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.JobScheduler;
import com.neo4j.bench.infra.JobStatus;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSBatchJobScheduler;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import com.neo4j.bench.model.util.JsonUtil;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class BenchmarkJobScheduler
{

    private static final Logger LOG = LoggerFactory.getLogger( BenchmarkJobScheduler.class );

    public static BenchmarkJobScheduler create( String jobQueue, String jobDefinition, String batchStack, AWSCredentials awsCredentials )
    {
        return new BenchmarkJobScheduler( AWSBatchJobScheduler.getJobScheduler( awsCredentials, jobQueue, jobDefinition, batchStack ), awsCredentials );
    }

    private static List<JobStatus> jobStatuses( JobScheduler jobScheduler,
                                                String awsRegion,
                                                Map<JobId,String> scheduledJobIds,
                                                Map<JobId,JobStatus> prevJobStatusMap )
    {
        List<JobStatus> jobStatuses = jobScheduler.jobsStatuses( Lists.newArrayList( scheduledJobIds.keySet() ) );

        Collection<JobStatus> prevJobStatus = prevJobStatusMap.values();

        // filter out job statuses which didn't change
        List<JobStatus> updatedJobsStatus = jobStatuses.stream().filter( Predicates.not( prevJobStatus::contains ) ).collect( toList() );

        // update job statuses
        prevJobStatusMap.putAll(
                updatedJobsStatus.stream().collect( toMap( status -> status.getJobId(), Function.identity() ) )
        );

        if ( !updatedJobsStatus.isEmpty() )
        {
            LOG.info( "updated jobs statuses:\n{}",
                      updatedJobsStatus.stream()
                                       .map( status -> jobStatusPrintout( awsRegion, scheduledJobIds, status ) )
                                       .collect( joining( "\t\n" ) ) );
        }

        return jobStatuses;
    }

    private static String jobStatusPrintout( String awsRegion, Map<JobId,String> scheduledJobIds, JobStatus status )
    {
        return format( "%s - %s", scheduledJobIds.get( status.getJobId() ), status.toStatusLine( awsRegion ) );
    }

    private final AWSCredentials awsCredentials;
    private final JobScheduler jobScheduler;
    // keeps map of scheduled job ids to job name
    private final Map<JobId,String> scheduledJobsId = new HashMap<>();

    private BenchmarkJobScheduler( JobScheduler awsBatchJobScheduler, AWSCredentials awsCredentials )
    {
        jobScheduler = awsBatchJobScheduler;
        this.awsCredentials = awsCredentials;
    }

    public void scheduleBenchmarkJob( String jobName,
                                      JobParams jobParams,
                                      Workspace workspace,
                                      URI artifactWorkerUri,
                                      File jobParameterJson )
            throws ArtifactStoreException
    {

        InfraParams infraParams = jobParams.infraParams();
        AWSS3ArtifactStorage artifactStorage = AWSS3ArtifactStorage.getAWSS3ArtifactStorage( infraParams );

        JsonUtil.serializeJson( jobParameterJson.toPath(), jobParams );

        URI artifactBaseURI = infraParams.artifactBaseUri();
        artifactStorage.uploadBuildArtifacts( artifactBaseURI, workspace );
        LOG.info( "upload build artifacts into {}", artifactBaseURI );

        // job name should follow these restrictions, https://docs.aws.amazon.com/cli/latest/reference/batch/submit-job.html
        // The first character must be alphanumeric, and up to 128 letters (uppercase and lowercase), numbers, hyphens, and underscores are allowed.
        String sanitizedJobName = StringUtils.substring( jobName.replaceAll( "[^\\p{Alnum}|^_|^-]", "_" ), 0, 127 );

        JobId jobId = jobScheduler.schedule( artifactWorkerUri, artifactBaseURI, sanitizedJobName );
        LOG.info( "job scheduled, with id {}", jobId.id() );
        scheduledJobsId.put( jobId, sanitizedJobName );
    }

    public void awaitFinished()
    {
        if ( scheduledJobsId.isEmpty() )
        {
            throw new IllegalStateException( "no benchmark jobs were scheduled" );
        }

        // wait until they are done, or fail
        RetryPolicy<List<JobStatus>> retries = new RetryPolicy<List<JobStatus>>()
                .handleResultIf( jobsStatuses -> jobsStatuses.stream().anyMatch( JobStatus::isWaiting ) )
                .withDelay( Duration.ofMinutes( 5 ) )
                .withMaxAttempts( -1 );

        Map<JobId,JobStatus> prevJobsStatus = new HashMap<>();
        List<JobStatus> jobsStatuses =
                Failsafe.with( retries )
                        .get( () -> BenchmarkJobScheduler
                                .jobStatuses( jobScheduler, awsCredentials.awsRegion(), scheduledJobsId, prevJobsStatus ) );
        LOG.info( "jobs are done with following statuses\n{}",
                  jobsStatuses.stream()
                              .map( status -> jobStatusPrintout( awsCredentials.awsRegion(), scheduledJobsId, status ) )
                              .collect( joining( "\n\t" ) ) );

        // if any of the jobs failed, fail whole run
        if ( jobsStatuses.stream().anyMatch( JobStatus::isFailed ) )
        {
            throw new RuntimeException( "there are failed jobs: \n" +
                                        jobsStatuses.stream()
                                                    .filter( JobStatus::isFailed )
                                                    .map( Object::toString )
                                                    .collect( joining( "\n" ) ) );
        }
    }
}
