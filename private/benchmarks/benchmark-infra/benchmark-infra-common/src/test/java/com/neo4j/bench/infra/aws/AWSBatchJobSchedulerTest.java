/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.neo4j.bench.infra.BenchmarkArgs;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

class AWSBatchJobSchedulerTest
{
    @Test
    void scheduleJob()
    {
        AWSBatch awsBatch = mock( AWSBatch.class );
        when( awsBatch.submitJob( Mockito.any() ) ).thenReturn( new SubmitJobResult().withJobId( "1" ) );
        // given
        AWSBatchJobScheduler jobScheduler = new AWSBatchJobScheduler( awsBatch, "job-queue", "job-definition" );
        List<String> parameters = Arrays.asList(
                "warmup_count",
                "measurement_count",
                "db_edition",
                "jvm",
                "profilers",
                "forks",
                "results_path",
                "time_unit",
                "results_store_uri",
                "results_store_user",
                "results_store_password",
                "neo4j_commit",
                "neo4j_version",
                "neo4j_branch",
                "neo4j_branch_owner",
                "tool_commit",
                "tool_branch_owner",
                "tool_branch",
                "teamcity_build",
                "parent_teamcity_build",
                "execution_mode",
                "jvm_args",
                "recreate_schema",
                "planner",
                "runtime",
                "triggered_by",
                "error_policy"
                );
        // when
        List<String> jobIds = jobScheduler.schedule(
                "musicbrainz",
                "musicbrainz",
                new BenchmarkArgs( parameters, URI.create( "s3://benchmarking.neohq.net/worker.jar" ) ) );
        // then

        assertEquals( asList( "1" ), jobIds );

        ArgumentCaptor<SubmitJobRequest> captor = ArgumentCaptor.forClass( SubmitJobRequest.class );
        verify( awsBatch ).submitJob( captor.capture() );

        assertEquals( "job-queue", captor.getValue().getJobQueue());
        assertEquals( "job-definition", captor.getValue().getJobDefinition());
        assertEquals( "macro-musicbrainz", captor.getValue().getJobName());

        MapDifference<String,String> entriesDiffering = Maps.difference(
                        captor.getValue().getParameters(),
                        parameters.stream().collect( toMap( identity(), identity()) )
                        );

        assertTrue( entriesDiffering.entriesDiffering().isEmpty() );
        assertTrue( ImmutableMap.of(
                "workerArtifactUri", "s3://benchmarking.neohq.net/worker.jar",
                "workload", "musicbrainz",
                "db", "musicbrainz").equals( entriesDiffering.entriesOnlyOnLeft()));
    }
}
