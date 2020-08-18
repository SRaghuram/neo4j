/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.neo4j.bench.infra.JobId;
import com.neo4j.bench.infra.JobStatus;
import org.junit.Test;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class BatchBenchmarkJobTest
{

    @Test
    public void jobStatusUpdateChangeTimestamps()
    {

        // given
        Clock clock = Clock.tickMinutes( ZoneId.systemDefault() );
        JobId jobId = new JobId( UUID.randomUUID().toString() );

        // when
        JobStatus jobStatus = new JobStatus( jobId,
                                             "SUBMITTED",
                                             null,
                                             null );
        BatchBenchmarkJob benchmarkJob = BatchBenchmarkJob.newJob( "newJobName", UUID.randomUUID().toString(), jobStatus, clock );

        // then
        assertEquals( "newJobName", benchmarkJob.jobName() );
        assertEquals( jobStatus, benchmarkJob.lastJobStatus() );
        ZonedDateTime queued = benchmarkJob.queuedAt();
        assertNotNull( queued );
        assertNull( benchmarkJob.runAt() );
        assertNull( benchmarkJob.doneAt() );

        // when
        jobStatus = new JobStatus( jobId,
                                   "PENDING",
                                   null,
                                   null );
        benchmarkJob = benchmarkJob.copyWith( jobStatus, clock );

        // then
        assertEquals( "newJobName", benchmarkJob.jobName() );
        assertEquals( jobStatus, benchmarkJob.lastJobStatus() );
        assertEquals( queued, benchmarkJob.queuedAt() );
        assertNull( benchmarkJob.runAt() );
        assertNull( benchmarkJob.doneAt() );

        // when
        jobStatus = new JobStatus( jobId,
                                   "RUNNABLE",
                                   null,
                                   null );
        benchmarkJob = benchmarkJob.copyWith( jobStatus, clock );

        // then
        assertEquals( "newJobName", benchmarkJob.jobName() );
        assertEquals( jobStatus, benchmarkJob.lastJobStatus() );
        assertEquals( queued, benchmarkJob.queuedAt() );
        assertNull( benchmarkJob.runAt() );
        assertNull( benchmarkJob.doneAt() );

        // when
        jobStatus = new JobStatus( jobId,
                                   "STARTING",
                                   null,
                                   null );
        benchmarkJob = benchmarkJob.copyWith( jobStatus, clock );

        // then
        assertEquals( "newJobName", benchmarkJob.jobName() );
        assertEquals( jobStatus, benchmarkJob.lastJobStatus() );
        assertEquals( queued, benchmarkJob.queuedAt() );
        assertNull( benchmarkJob.runAt() );
        assertNull( benchmarkJob.doneAt() );

        // when
        jobStatus = new JobStatus( jobId,
                                   "RUNNING",
                                   null,
                                   null );
        benchmarkJob = benchmarkJob.copyWith( jobStatus, clock );

        // then
        assertEquals( "newJobName", benchmarkJob.jobName() );
        assertEquals( jobStatus, benchmarkJob.lastJobStatus() );
        assertEquals( queued, benchmarkJob.queuedAt() );
        ZonedDateTime running = benchmarkJob.runAt();
        assertNotNull( running );
        assertNull( benchmarkJob.doneAt() );

        // when
        jobStatus = new JobStatus( jobId,
                                   "SUCCEEDED",
                                   null,
                                   null );
        benchmarkJob = benchmarkJob.copyWith( jobStatus, clock );

        // then
        assertEquals( "newJobName", benchmarkJob.jobName() );
        assertEquals( jobStatus, benchmarkJob.lastJobStatus() );
        assertEquals( queued, benchmarkJob.queuedAt() );
        assertEquals( running, benchmarkJob.runAt() );
        assertNotNull( benchmarkJob.doneAt() );
    }
}
