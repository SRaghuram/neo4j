/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.process.JvmArgs;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.tool.macro.RunWorkloadParams;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.infra.JobId;
import com.neo4j.bench.infra.commands.BatchJobCommandParameters;
import com.neo4j.bench.infra.commands.InfraParams;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AWSBatchJobSchedulerTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void scheduleJob() throws Exception
    {
        // given
        AWSBatch awsBatch = mock( AWSBatch.class );
        String jobId = "1";
        when( awsBatch.submitJob( Mockito.any() ) ).thenReturn( new SubmitJobResult().withJobId( jobId ) );
        AWSBatchJobScheduler jobScheduler = new AWSBatchJobScheduler( awsBatch, "job-queue", "job-definition" );

        Path jvmPath = Paths.get( Jvm.defaultJvmOrFail().launchJava() );

        RunWorkloadParams runWorkloadParams = new RunWorkloadParams(
                "musicbrainz",
                Edition.ENTERPRISE,
                jvmPath,
                Lists.newArrayList( ProfilerType.GC, ProfilerType.ASYNC ),
                1,//warmup count
                1, // measurement count
                Duration.ofSeconds( 10 ), // min duration
                Duration.ofSeconds( 10 ), // max duration
                1, // measurement forks
                TimeUnit.MICROSECONDS,
                Runtime.DEFAULT,
                Planner.DEFAULT,
                ExecutionMode.EXECUTE,
                JvmArgs.from( "-Xms4g", "-Xmx4g" ),
                false,// recreate schema
                false, // skip flame graphs
                Deployment.embedded(),
                "1234567", // neo4j commit
                "3.2.1",// neo4j version
                "3.2", // neo4j branch
                "neo4j", // neo4j branch owner
                "1234567", // tool commit
                "neo4j", // tool branch owner
                "3.2", // tool branch
                1L, // build
                2L, // parent build
                "teamcity" // triggered by
        );

        Path workspaceDir = temporaryFolder.newFolder().toPath();
        String awsSecret = null;
        String awsKey = null;
        String awsRegion = "eu-north-1";
        String storeName = "musicbrainz";
        String resultsStoreUsername = "user";
        String resultsStorePassword = "password";
        URI resultsStoreUri = URI.create( "https://store.place" );
        URI workerArtifactUri = URI.create( "s3://benchmarking.neohq.net/worker.jar" );
        URI baseArtifactUri = URI.create( "s3://benchmarking.neohq.net/" );

        InfraParams infraParams = new InfraParams(
                awsSecret,
                awsKey,
                awsRegion,
                storeName,
                resultsStoreUsername,
                resultsStorePassword,
                resultsStoreUri,
                baseArtifactUri,
                ErrorReportingPolicy.FAIL );

        Map<String,String> expectedParams = new HashMap<>();
        expectedParams.put( InfraParams.CMD_ARTIFACT_WORKER_URI, workerArtifactUri.toString() );
        expectedParams.put( InfraParams.CMD_ARTIFACT_BASE_URI, baseArtifactUri.toString() );

        // when
        JobId scheduleJobId = jobScheduler.schedule(
                workerArtifactUri,
                baseArtifactUri,
                runWorkloadParams );

        // then
        assertEquals( new JobId( jobId ), scheduleJobId, "invalid job id in submit job request response" );

        ArgumentCaptor<SubmitJobRequest> jobRequestCaptor = ArgumentCaptor.forClass( SubmitJobRequest.class );
        verify( awsBatch ).submitJob( jobRequestCaptor.capture() );
        SubmitJobRequest jobRequest = jobRequestCaptor.getValue();
        Map<String,String> jobRequestParameters = jobRequest.getParameters();

        assertEquals( "job-queue", jobRequest.getJobQueue() );
        assertEquals( "job-definition", jobRequest.getJobDefinition() );
        assertEquals( "macro-musicbrainz-3_2_1-teamcity", jobRequest.getJobName() );

        assertEquals( expectedParams, jobRequestParameters );

        assertEquals( Collections.emptySet(),
                      Sets.difference(
                              new HashSet<>( BatchJobCommandParameters.getBatchJobCommandParameters() ),
                              jobRequestParameters.keySet() ) );
    }
}
