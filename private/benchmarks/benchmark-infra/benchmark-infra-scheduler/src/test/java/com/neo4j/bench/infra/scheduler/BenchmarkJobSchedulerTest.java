/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.google.common.collect.Queues;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.submit.CreateJob;
import com.neo4j.bench.common.results.ErrorReportingPolicy;
import com.neo4j.bench.infra.AWSCredentials;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.BenchmarkingRun;
import com.neo4j.bench.infra.BenchmarkingTool;
import com.neo4j.bench.infra.BenchmarkingToolRunner;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.JobId;
import com.neo4j.bench.infra.JobParams;
import com.neo4j.bench.infra.JobScheduler;
import com.neo4j.bench.infra.JobStatus;
import com.neo4j.bench.infra.ResultStoreCredentials;
import com.neo4j.bench.infra.Workspace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BenchmarkJobSchedulerTest
{

    @Mock
    private JobScheduler jobScheduler;

    @Mock
    private ArtifactStorage artifactStorage;
    private AWSCredentials awsCredentials;
    private Workspace workspace;
    private BenchmarkJobScheduler benchmarkJobScheduler;

    @BeforeEach
    public void setUp( @TempDir Path tempDir ) throws Exception
    {
        MockitoAnnotations.initMocks( this );

        awsCredentials = new AWSCredentials( "awsAccessKeyId",
                                             "awsSecretAccessKey",
                                             "awsRegion" );

        File workDir = Files.createTempDirectory( tempDir, "work_dir" ).toFile();
        workspace = Workspace.create( workDir.toPath() ).build();
        benchmarkJobScheduler = BenchmarkJobScheduler.create( jobScheduler, artifactStorage, awsCredentials, Duration.ofSeconds( 1 ) );
    }

    @Test
    @Timeout( 10_000 )
    public void scheduleJobAwaitFinishWhenSucceededAndReport( @TempDir Path tempDir ) throws Exception
    {
        // given
        JobId jobId = new JobId( UUID.randomUUID().toString() );
        String testRunId = UUID.randomUUID().toString();
        String jobName = "jobName";

        when(
                jobScheduler.schedule(
                        URI.create( "http://localhost/worker.jar" ),
                        URI.create( "http://localhost/artifact/" ),
                        jobName ) )
                .thenReturn( jobId );

        LinkedBlockingDeque<String> jobStatuses = Queues.newLinkedBlockingDeque( asList( "SUBMITTED", "RUNNING", "SUCCEEDED" ) );
        when( jobScheduler.jobsStatuses( anyList() ) )
                .then( (Answer<List<JobStatus>>) invocation ->
                        singletonList( new JobStatus( jobId, jobStatuses.take(), null, null ) ) );

        JobParams<NoopBenchmarkingToolRunnerParams> jobParams = getJobParams( testRunId );

        // when
        JobId actualjobId = benchmarkJobScheduler.scheduleBenchmarkJob( jobName,
                                                                        jobParams,
                                                                        workspace,
                                                                        URI.create( "http://localhost/worker.jar" ) );
        // then
        assertEquals( jobId, actualjobId );

        // when
        Collection<BatchBenchmarkJob> benchmarkJobs = benchmarkJobScheduler.awaitFinished();

        // then
        BatchBenchmarkJob benchmarkJob = benchmarkJobs.stream().findFirst().get();

        assertBenchmarkJob( jobId, jobName, benchmarkJob );
        assertTrue( benchmarkJob.lastJobStatus().isDone() );
        assertFalse( benchmarkJob.lastJobStatus().isFailed() );

        // when
        StoreClient storeClient = mock( StoreClient.class );
        benchmarkJobScheduler.reportJobsTo( storeClient );

        // then
        ArgumentCaptor<CreateJob> varArgs = ArgumentCaptor.forClass( CreateJob.class );
        verify( storeClient ).execute( varArgs.capture() );

        CreateJob actualCreateJob = varArgs.getValue();
        assertCreateJob( benchmarkJob, testRunId, actualCreateJob );
    }

    @Timeout( 10_000 )
    public void scheduleJobWithJobRequestConsumerAwaitFinishWhenSucceededAndReport() throws Exception
    {
        // given
        JobId jobId = new JobId( UUID.randomUUID().toString() );
        String testRunId = UUID.randomUUID().toString();
        String jobName = "jobName";
        String jobParametersJson = "custom-job-paramters.json";
        Optional<JobScheduler.JobRequestConsumer> jobRequestConsumer = Optional.of( submitJobRequest ->
                                                                                    {
                                                                                    } );

        when( jobScheduler.schedule(
                URI.create( "http://localhost/worker.jar" ),
                URI.create( "http://localhost/artifact/" ),
                jobName,
                jobParametersJson,
                jobRequestConsumer
        ) ).thenReturn( jobId );

        LinkedBlockingDeque<String> jobStatuses = Queues.newLinkedBlockingDeque( asList( "SUBMITTED", "RUNNING", "SUCCEEDED" ) );
        when( jobScheduler.jobsStatuses( anyList() ) )
                .then( (Answer<List<JobStatus>>) invocation ->
                        singletonList( new JobStatus( jobId, jobStatuses.take(), null, null ) ) );

        JobParams<NoopBenchmarkingToolRunnerParams> jobParams = getJobParams( testRunId );

        // when
        JobId actualjobId = benchmarkJobScheduler.scheduleBenchmarkJob( jobName,
                                                                        jobParams,
                                                                        workspace,
                                                                        URI.create( "http://localhost/worker.jar" ),
                                                                        jobParametersJson,
                                                                        jobRequestConsumer );
        // then
        assertEquals( jobId, actualjobId );

        // when
        Collection<BatchBenchmarkJob> benchmarkJobs = benchmarkJobScheduler.awaitFinished();

        // then
        BatchBenchmarkJob benchmarkJob = benchmarkJobs.stream().findFirst().get();

        assertBenchmarkJob( jobId, jobName, benchmarkJob );
        assertTrue( benchmarkJob.lastJobStatus().isDone() );
        assertFalse( benchmarkJob.lastJobStatus().isFailed() );

        // when
        StoreClient storeClient = mock( StoreClient.class );
        benchmarkJobScheduler.reportJobsTo( storeClient );

        // then
        ArgumentCaptor<CreateJob> varArgs = ArgumentCaptor.forClass( CreateJob.class );
        verify( storeClient ).execute( varArgs.capture() );

        CreateJob actualCreateJob = varArgs.getValue();
        assertCreateJob( benchmarkJob, testRunId, actualCreateJob );
    }

    @Test
    @Timeout( 10_000 )
    public void scheduleJobAwaitFinishWhenFailedAndReport( @TempDir Path tempDir ) throws Exception
    {
        // given
        JobId jobId = new JobId( UUID.randomUUID().toString() );
        String testRunId = UUID.randomUUID().toString();
        String jobName = "jobName";

        when(
                jobScheduler.schedule(
                        URI.create( "http://localhost/worker.jar" ),
                        URI.create( "http://localhost/artifact/" ),
                        jobName ) )
                .thenReturn( jobId );

        LinkedBlockingDeque<String> jobStatuses = Queues.newLinkedBlockingDeque( asList( "SUBMITTED", "RUNNING", "FAILED" ) );
        when( jobScheduler.jobsStatuses( anyList() ) )
                .then( (Answer<List<JobStatus>>) invocation -> singletonList( new JobStatus( jobId, jobStatuses.take(), null, null ) ) );

        JobParams<NoopBenchmarkingToolRunnerParams> jobParams = getJobParams( testRunId );

        // when
        JobId actualjobId = benchmarkJobScheduler.scheduleBenchmarkJob( jobName,
                                                                        jobParams,
                                                                        workspace,
                                                                        URI.create( "http://localhost/worker.jar" ) );
        // then
        assertEquals( jobId, actualjobId );

        // when
        Collection<BatchBenchmarkJob> benchmarkJobs = Collections.emptyList();
        try
        {
            benchmarkJobScheduler.awaitFinished();
            fail( "should throw exception when there are failed jobs" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( BenchmarkJobFailedException.class ) );
            assertThat( e.getMessage(), startsWith( "there are failed jobs:" ) );
            benchmarkJobs = ((BenchmarkJobFailedException) e).benchmarkJobs();
        }

        // then
        BatchBenchmarkJob benchmarkJob = benchmarkJobs.stream().findFirst().get();

        assertBenchmarkJob( jobId, jobName, benchmarkJob );
        assertTrue( benchmarkJob.lastJobStatus().isDone() );
        assertTrue( benchmarkJob.lastJobStatus().isFailed() );

        // when
        StoreClient storeClient = mock( StoreClient.class );
        benchmarkJobScheduler.reportJobsTo( storeClient );

        // then
        ArgumentCaptor<CreateJob> varArgs = ArgumentCaptor.forClass( CreateJob.class );
        verify( storeClient ).execute( varArgs.capture() );

        CreateJob actualCreateJob = varArgs.getValue();
        assertCreateJob( benchmarkJob, testRunId, actualCreateJob );
    }

    private static void assertCreateJob( BatchBenchmarkJob expectedBenchmarkJob, String expectedTestRunId, CreateJob actualCreateJob )
    {
        assertEquals( expectedBenchmarkJob.lastJobStatus().jobId().id(), actualCreateJob.job().id() );
        assertEquals( expectedBenchmarkJob.runAt().toEpochSecond(), actualCreateJob.job().runAt().longValue() );
        assertEquals( expectedBenchmarkJob.doneAt().toEpochSecond(), actualCreateJob.job().doneAt().longValue() );
        assertEquals( expectedBenchmarkJob.queuedAt().toEpochSecond(), actualCreateJob.job().queuedAt().longValue() );
        assertEquals( expectedTestRunId, actualCreateJob.testRunId() );
    }

    private static void assertBenchmarkJob( JobId expectedJobId, String expectedJobName, BatchBenchmarkJob actualBenchmarkJob )
    {
        assertEquals( expectedJobName, actualBenchmarkJob.jobName() );
        assertEquals( expectedJobId, actualBenchmarkJob.lastJobStatus().jobId() );
        assertTrue( actualBenchmarkJob.queuedAt().isBefore( actualBenchmarkJob.runAt() ) );
        assertTrue( actualBenchmarkJob.runAt().isBefore( actualBenchmarkJob.doneAt() ) );
    }

    private JobParams<NoopBenchmarkingToolRunnerParams> getJobParams( String testRunId )
    {
        return new JobParams<>( new InfraParams( awsCredentials,
                                                 "resultsStoreUsername",
                                                 "resultsStorePasswordSecretName",
                                                 URI.create( "http://localhost" ),
                                                 URI.create( "http://localhost/artifact/" ),
                                                 ErrorReportingPolicy.FAIL,
                                                 workspace ),
                                new BenchmarkingRun<>(
                                        new BenchmarkingTool<>( NoopBenchmarkingToolRunner.class,
                                                                new NoopBenchmarkingToolRunnerParams() ),
                                        testRunId ) );
    }

    public static class NoopBenchmarkingToolRunner implements BenchmarkingToolRunner<NoopBenchmarkingToolRunnerParams>
    {
        @Override
        public void runTool( JobParams<NoopBenchmarkingToolRunnerParams> jobParams,
                             ArtifactStorage artifactStorage,
                             Path workspacePath,
                             Workspace artifactsWorkspace,
                             ResultStoreCredentials resultStoreCredentials,
                             URI artifactBaseUri )
        {
        }
    }

    public static class NoopBenchmarkingToolRunnerParams
    {
    }
}
