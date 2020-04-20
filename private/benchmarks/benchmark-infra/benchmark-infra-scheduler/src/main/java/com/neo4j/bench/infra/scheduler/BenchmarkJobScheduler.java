/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.neo4j.bench.common.util.JsonUtil;
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
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.joining;

public class BenchmarkJobScheduler
{

    private static final Logger LOG = LoggerFactory.getLogger( BenchmarkJobScheduler.class );

    public static BenchmarkJobScheduler create( String jobQueue, String jobDefinition, String batchStack, AWSCredentials awsCredentials )
    {
        return new BenchmarkJobScheduler( AWSBatchJobScheduler.getJobScheduler( awsCredentials, jobQueue, jobDefinition, batchStack ), awsCredentials );
    }

    private static List<JobStatus> jobStatuses( JobScheduler jobScheduler, String awsRegion, List<JobId> jobIds )
    {
        List<JobStatus> jobStatuses = jobScheduler.jobsStatuses( jobIds );
        LOG.info( "current jobs statuses:\n{}",
                  jobStatuses.stream()
                             .map( status -> status.toStatusLine( awsRegion ) ).collect( joining( "\n" ) ) );
        return jobStatuses;
    }

    private final AWSCredentials awsCredentials;
    private final JobScheduler jobScheduler;
    private final List<JobId> scheduledJobsId = new ArrayList<>();

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
        scheduledJobsId.add( jobId );
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

        List<JobStatus> jobsStatuses =
                Failsafe.with( retries ).get( () -> BenchmarkJobScheduler.jobStatuses( jobScheduler, awsCredentials.awsRegion(), scheduledJobsId ) );
        LOG.info( "jobs are done with following statuses {}", jobsStatuses.stream().map( Object::toString ).collect( joining( "\n" ) ) );

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
