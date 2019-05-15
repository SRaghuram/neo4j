/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.amazonaws.SdkClientException;
import com.neo4j.bench.infra.ArtifactStoreException;
import com.neo4j.bench.infra.BenchmarkArgs;
import com.neo4j.bench.infra.InfraCommand;
import com.neo4j.bench.infra.JobScheduler;
import com.neo4j.bench.infra.JobStatus;
import com.neo4j.bench.infra.Workspace;
import com.neo4j.bench.infra.aws.AWSBatchJobScheduler;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

@Command( name = "schedule-macro" )
public class ScheduleMacro extends InfraCommand
{

    private static final Logger LOG = LoggerFactory.getLogger( ScheduleMacro.class );

    @Option( type = OptionType.COMMAND, name = "--workloads", required = true )
    private String workloads;

    @Option( type = OptionType.COMMAND, name = "--dbs", required = true )
    private String dbs;

    @Option( type = OptionType.COMMAND, name = "--workerArtifactUri", required = true )
    private String workerArtifactUri;

    @Option( type = OptionType.COMMAND, name = "--jobQueue", arity = 1, required = false )
    private String jobQueue = "macro-benchnmark-run-queue";

    @Option( type = OptionType.COMMAND, name = "--jobDefinition", required = false )
    private String jobDefinition = "macro-benchmark-job-definition";

    @Arguments
    private List<String> parameters;

    @Override
    public void run()
    {
        try
        {
            BenchmarkArgs benchmarkArgs = new BenchmarkArgs( parameters, URI.create( workerArtifactUri ) );
            Workspace workspace = Workspace.create(
                    Paths.get( workspacePath ).toAbsolutePath(),
                    // required artifacts
                    Paths.get( "benchmark-infra-scheduler.jar" ),
                    Paths.get( format( "neo4j-%s-%s-unix.tar.gz", benchmarkArgs.getDbEdition().toLowerCase(), benchmarkArgs.getNeo4jVersion() ) ),
                    Paths.get( "macro/target/macro.jar" ),
                    Paths.get( "macro/run-report-benchmarks.sh" ) );

            String buildID = benchmarkArgs.getTeamcityBuild();

            AWSS3ArtifactStorage artifactStorage = AWSS3ArtifactStorage.create( awsRegion, awsKey, awsSecret );
            artifactStorage.verifyBuildArtifactsExpirationRule();

            JobScheduler jobScheduler = AWSBatchJobScheduler.create( awsRegion, awsKey, awsSecret, jobQueue, jobDefinition );

            URI buildArtifactsUri = artifactStorage.uploadBuildArtifacts( buildID, workspace );

            LOG.info( "upload build artifacts into {}", buildArtifactsUri );

            // schedule them
            List<String> jobIds = jobScheduler.schedule( workloads, dbs, benchmarkArgs );
            LOG.info( "jobs are scheduled, with ids\n{}", jobIds.stream().collect( joining( "\n" ) ) );
            // wait until they are done, or fail
            RetryPolicy<List<JobStatus>> retries = new RetryPolicy<List<JobStatus>>()
                    .handleResultIf( jobsStatuses -> jobsStatuses.stream().anyMatch( JobStatus::isWaiting ) )
                    .withDelay( Duration.ofMinutes( 5 ))
                    .withMaxAttempts( -1 );

            List<JobStatus> jobsStatuses = Failsafe.with( retries ).get( () -> jobScheduler.jobsStatuses(jobIds));
            LOG.info( "jobs are done with following statuses\n{}", jobsStatuses.stream().map(Object::toString).collect( joining( "\n" ) ) );

        }
        catch ( SdkClientException | ArtifactStoreException e )
        {
            LOG.error( "failed to schedule benchmarking job", e );
        }

    }

}
