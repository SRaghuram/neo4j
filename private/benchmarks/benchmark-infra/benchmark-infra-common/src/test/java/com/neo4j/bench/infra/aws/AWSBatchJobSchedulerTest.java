/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.model.JobTimeout;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobId;
import com.neo4j.bench.infra.JobScheduler.JobRequestConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.neo4j.bench.common.tool.macro.RunMacroWorkloadParams.CMD_JOB_PARAMETERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AWSBatchJobSchedulerTest
{

    @Test
    public void scheduleJob()
    {
        // given
        AWSBatch awsBatch = mock( AWSBatch.class );
        String jobId = "1";
        when( awsBatch.submitJob( Mockito.any() ) ).thenReturn( new SubmitJobResult().withJobId( jobId ) );
        AWSBatchJobScheduler jobScheduler = new AWSBatchJobScheduler( awsBatch, "job-queue", "job-definition" );

        String storeName = "musicbrainz";
        String jobParameters = "jobParameters";
        URI workerArtifactUri = URI.create( "s3://benchmarking.neohq.net/worker.jar" );
        URI baseArtifactUri = URI.create( "s3://benchmarking.neohq.net/" );

        Map<String,String> expectedParams = new HashMap<>();
        expectedParams.put( InfraParams.CMD_ARTIFACT_WORKER_URI, workerArtifactUri.toString() );
        expectedParams.put( InfraParams.CMD_ARTIFACT_BASE_URI, baseArtifactUri.toString() );
        expectedParams.put( CMD_JOB_PARAMETERS, jobParameters );
        JobTimeout timeout = new JobTimeout().withAttemptDurationSeconds( 10 );

        // when
        String jobName = String.format( "%s-%s-%s-%s", "macro", storeName, "3_4_15", "neo4j" );
        JobRequestConsumer consumer = jobRequest -> jobRequest.withTimeout( timeout );

        JobId scheduleJobId = jobScheduler.schedule(
                workerArtifactUri,
                baseArtifactUri,
                jobName,
                jobParameters,
                Optional.of( consumer )
        );

        // then
        assertEquals( new JobId( jobId ), scheduleJobId, "invalid job id in submit job request response" );

        ArgumentCaptor<SubmitJobRequest> jobRequestCaptor = ArgumentCaptor.forClass( SubmitJobRequest.class );
        verify( awsBatch ).submitJob( jobRequestCaptor.capture() );
        SubmitJobRequest jobRequest = jobRequestCaptor.getValue();
        Map<String,String> jobRequestParameters = jobRequest.getParameters();

        assertEquals( "job-queue", jobRequest.getJobQueue() );
        assertEquals( "job-definition", jobRequest.getJobDefinition() );
        assertEquals( "macro-musicbrainz-3_4_15-neo4j", jobRequest.getJobName() );
        assertEquals( expectedParams, jobRequestParameters );
        assertEquals( timeout, jobRequest.getTimeout() );
    }
}
