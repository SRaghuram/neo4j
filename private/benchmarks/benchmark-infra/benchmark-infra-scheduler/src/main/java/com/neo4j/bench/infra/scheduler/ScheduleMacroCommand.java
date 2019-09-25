/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.amazonaws.SdkClientException;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.JobId;
import com.neo4j.bench.infra.JobScheduler;
import com.neo4j.bench.infra.JobStatus;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSBatchJobLogs;
import com.neo4j.bench.infra.aws.AWSBatchJobScheduler;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import com.neo4j.bench.infra.commands.BaseInfraCommand;
import com.neo4j.bench.infra.commands.InfraParams;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.joining;

@Command( name = "schedule-macro" )
public class ScheduleMacroCommand extends BaseInfraCommand
{
    private static final Logger LOG = LoggerFactory.getLogger( ScheduleMacroCommand.class );

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_JOB_QUEUE,
             title = "AWS Batch Job Queue Name" )
    private String jobQueue = "macro-benchmark-run-queue";

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_JOB_DEFINITION,
             title = "AWS Batch Job Definition Name" )
    private String jobDefinition = "macro-benchmark-job-definition";

    @Option( type = OptionType.COMMAND,
             name = InfraParams.CMD_BATCH_STACK,
             title = "AWS Batch Stack Name" )
    private String batchStack = "benchmarking";

    @Override
    protected void doRunInfra( RunWorkloadParams runWorkloadParams, InfraParams infraParams )
    {
        try
        {
            Workspace workspace = Workspace.assertMacroWorkspace( infraParams.workspaceDir(),
                                                                  runWorkloadParams.neo4jEdition(),
                                                                  runWorkloadParams.neo4jVersion() );

            AWSS3ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( infraParams.awsRegion(),
                                                                                infraParams.awsKey(),
                                                                                infraParams.awsSecret() );
            artifactStorage.verifyBuildArtifactsExpirationRule( infraParams.artifactBaseUri() );
            URI buildArtifactsUri = artifactStorage.uploadBuildArtifacts( infraParams.artifactBaseUri(), workspace );
            LOG.info( "upload build artifacts into {}", buildArtifactsUri );

            JobScheduler jobScheduler = AWSBatchJobScheduler.create( infraParams.awsRegion(),
                                                                     infraParams.awsKey(),
                                                                     infraParams.awsSecret(),
                                                                     jobQueue,
                                                                     jobDefinition,
                                                                     batchStack );
            JobId jobId = jobScheduler.schedule( infraParams.artifactWorkerUri(), infraParams.artifactBaseUri(), infraParams, runWorkloadParams );
            LOG.info( "job scheduled, with id {} and logs stream at {}", jobId.id(), AWSBatchJobLogs.getLogStreamName( jobDefinition, jobId ) );
            // wait until they are done, or fail
            RetryPolicy<List<JobStatus>> retries = new RetryPolicy<List<JobStatus>>()
                    .handleResultIf( jobsStatuses -> jobsStatuses.stream().anyMatch( JobStatus::isWaiting ) )
                    .withDelay( Duration.ofMinutes( 5 ) )
                    .withMaxAttempts( -1 );

            List<JobStatus> jobsStatuses = Failsafe.with( retries ).get( () -> jobScheduler.jobsStatuses( Collections.singletonList( jobId ) ) );
            LOG.info( "jobs are done with following statuses\n{}", jobsStatuses.stream().map( Object::toString ).collect( joining( "\n" ) ) );

            // if any of the jobs failed, fail whole run
            if ( jobsStatuses.stream()
                    .filter( JobStatus::isFailed )
                    .count() != 0 )
            {
                throw new RuntimeException( "there are failed jobs" );
            }

        }
        catch ( SdkClientException | ArtifactStoreException e )
        {
            throw new RuntimeException( "failed to schedule benchmarking job", e );
        }
    }
}
