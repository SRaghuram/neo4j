/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.submit.CreateJob;
import com.neo4j.bench.infra.AWSCredentials;
import com.neo4j.bench.infra.ArtifactStorage;
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
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class BenchmarkJobScheduler
{

    private static final Logger LOG = LoggerFactory.getLogger( BenchmarkJobScheduler.class );
    private static final Duration DEFAULT_JOB_STATUS_CHECK_DELAY = Duration.ofSeconds( 5 );

    public static BenchmarkJobScheduler create( String jobQueue, String jobDefinition, String batchStack, AWSCredentials awsCredentials )
    {
        return new BenchmarkJobScheduler( AWSBatchJobScheduler.getJobScheduler( awsCredentials, jobQueue, jobDefinition, batchStack ),
                                          AWSS3ArtifactStorage.create( awsCredentials ),
                                          awsCredentials,
                                          DEFAULT_JOB_STATUS_CHECK_DELAY );
    }

    // for testing
    static BenchmarkJobScheduler create( JobScheduler jobScheduler,
                                         ArtifactStorage artifactStorage,
                                         AWSCredentials awsCredentials,
                                         Duration jobStatusCheckDelay )
    {
        return new BenchmarkJobScheduler( jobScheduler, artifactStorage, awsCredentials, jobStatusCheckDelay );
    }

    private static List<JobStatus> jobStatuses( JobScheduler jobScheduler,
                                                String awsRegion,
                                                Map<JobId,BatchBenchmarkJob> scheduledJobs,
                                                Map<JobId,JobStatus> prevJobStatusMap )
    {

        if ( scheduledJobs.isEmpty() )
        {
            return Collections.emptyList();
        }

        List<JobId> jobIds = Lists.newArrayList( scheduledJobs.keySet() );
        LOG.debug( "checking for jobs {} statuses", jobIds );
        List<JobStatus> jobStatuses = jobScheduler.jobsStatuses( jobIds );
        LOG.debug( "job statuses {}", jobStatuses );

        //copyWith jobs
        for ( JobStatus status : jobStatuses )
        {
            scheduledJobs.computeIfPresent( status.jobId(), ( key, oldValue ) -> oldValue.copyWith( status, Clock.systemDefaultZone() ) );
        }

        Collection<JobStatus> prevJobStatus = prevJobStatusMap.values();

        // filter out job statuses which didn't change
        List<JobStatus> updatedJobsStatus = jobStatuses.stream().filter( Predicates.not( prevJobStatus::contains ) ).collect( toList() );

        // update jobs status
        updatedJobsStatus.forEach( status -> prevJobStatusMap.put( status.jobId(), status ) );

        if ( !updatedJobsStatus.isEmpty() )
        {
            LOG.info( "updated jobs statuses:\n{}",
                      updatedJobsStatus.stream()
                                       .map( status -> jobStatusPrintout( awsRegion, scheduledJobs, status ) )
                                       .collect( joining( "\t\n" ) ) );
        }

        return jobStatuses;
    }

    private static String jobStatusPrintout( String awsRegion, Map<JobId,BatchBenchmarkJob> scheduledJobIds, JobStatus status )
    {
        return format( "%s - %s", scheduledJobIds.get( status.jobId() ).jobName(), status.toStatusLine( awsRegion ) );
    }

    private final AWSCredentials awsCredentials;
    private final JobScheduler jobScheduler;
    private final ArtifactStorage artifactStorage;
    private final ConcurrentMap<JobId,BatchBenchmarkJob> scheduledJobs = new ConcurrentHashMap<>();
    private final Duration jobStatusCheckDelay;

    private BenchmarkJobScheduler( JobScheduler jobScheduler, ArtifactStorage artifactStorage, AWSCredentials awsCredentials, Duration jobStatusCheckDelay )
    {
        this.jobScheduler = jobScheduler;
        this.artifactStorage = artifactStorage;
        this.awsCredentials = awsCredentials;
        this.jobStatusCheckDelay = jobStatusCheckDelay;
    }

    public JobId scheduleBenchmarkJob( String jobName,
                                       JobParams jobParams,
                                       Workspace workspace,
                                       URI artifactWorkerUri,
                                       File jobParameterJson )
            throws ArtifactStoreException
    {

        InfraParams infraParams = jobParams.infraParams();

        JsonUtil.serializeJson( jobParameterJson.toPath(), jobParams );

        URI artifactBaseURI = infraParams.artifactBaseUri();
        artifactStorage.uploadBuildArtifacts( artifactBaseURI, workspace );
        LOG.info( "uploaded build artifacts into {}", artifactBaseURI );

        // job name should follow these restrictions, https://docs.aws.amazon.com/cli/latest/reference/batch/submit-job.html
        // The first character must be alphanumeric, and up to 128 letters (uppercase and lowercase), numbers, hyphens, and underscores are allowed.
        String sanitizedJobName = StringUtils.substring( jobName.replaceAll( "[^\\p{Alnum}|^_-]", "_" ), 0, 127 );

        JobId jobId = jobScheduler.schedule( artifactWorkerUri, artifactBaseURI, sanitizedJobName );
        LOG.info( "job scheduled, with id {}", jobId.id() );
        scheduledJobs.put( jobId, BatchBenchmarkJob.newJob( jobName,
                                                            jobParams.benchmarkingRun().testRunId(),
                                                            new JobStatus( jobId, null, null, null ),
                                                            Clock.systemDefaultZone() ) );
        return jobId;
    }

    public Collection<BatchBenchmarkJob> awaitFinished()
    {

        RetryPolicy<List<JobStatus>> retries = new RetryPolicy<List<JobStatus>>()
                .handleResultIf( jobsStatuses -> jobsStatuses.isEmpty() || jobsStatuses.stream().anyMatch( JobStatus::isWaiting ) )
                .withDelay( jobStatusCheckDelay )
                .withMaxAttempts( -1 );

        ConcurrentMap<JobId,JobStatus> prevJobsStatus = new ConcurrentHashMap<>();
        List<JobStatus> jobsStatuses =
                Failsafe.with( retries )
                        .get( () -> BenchmarkJobScheduler
                                .jobStatuses( jobScheduler, awsCredentials.awsRegion(), scheduledJobs, prevJobsStatus ) );

        LOG.info( "jobs are done with following statuses\n{}",
                  jobsStatuses.stream()
                              .map( status -> jobStatusPrintout( awsCredentials.awsRegion(), scheduledJobs, status ) )
                              .collect( joining( "\n\t" ) ) );

        // if any of the jobs failed, fail whole run
        if ( jobsStatuses.stream().anyMatch( JobStatus::isFailed ) )
        {
            throw new BenchmarkJobFailedException( scheduledJobs.values() );
        }

        return scheduledJobs.values();
    }

    public void reportJobsTo( StoreClient storeClient )
    {
        for ( BatchBenchmarkJob benchmarkJob : scheduledJobs.values() )
        {
            CreateJob createJob = new CreateJob( benchmarkJob.toJob(),
                                                 benchmarkJob.testRunId() );
            storeClient.execute( createJob );
        }
    }
}
