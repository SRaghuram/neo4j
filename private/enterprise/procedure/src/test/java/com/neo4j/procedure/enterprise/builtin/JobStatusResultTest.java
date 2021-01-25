/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import org.neo4j.common.Subject;
import org.neo4j.scheduler.JobType;
import org.neo4j.scheduler.MonitoredJobInfo;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.Subject.SYSTEM;
import static org.neo4j.scheduler.Group.INDEX_POPULATION;
import static org.neo4j.scheduler.JobType.IMMEDIATE;
import static org.neo4j.scheduler.MonitoredJobInfo.State.SCHEDULED;

class JobStatusResultTest
{
    @Test
    void testImmediateJobWithDatabaseAndUser()
    {
        var jobInfo = new MonitoredJobInfo( 99, INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), new Subject( "user 1" ), "db 1",
                "something very useful", null, null, SCHEDULED, IMMEDIATE, "Pretty impressive progress" );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertJob( jobStatusResult, "job-99", "IndexPopulationMain", "2020-06-24T18:00:00Z", "db 1", "user 1", "something very useful", "IMMEDIATE", "", "",
                "SCHEDULED", "Pretty impressive progress" );
    }

    @Test
    void testImmediateJobWithoutDatabaseAndUser()
    {
        var jobInfo = new MonitoredJobInfo( 11, INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), SYSTEM, null, "something very useful", null, null,
                MonitoredJobInfo.State.EXECUTING, IMMEDIATE, null );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertJob( jobStatusResult, "job-11", "IndexPopulationMain", "2020-06-24T18:00:00Z", "", "", "something very useful", "IMMEDIATE", "", "",
                "EXECUTING", "" );
    }

    @Test
    void testDelayed()
    {
        var jobInfo = new MonitoredJobInfo( 22, INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), new Subject( "user 1" ), "db 1",
                "something very useful", Instant.parse( "2020-06-24T18:10:00Z" ), null, SCHEDULED, JobType.DELAYED, null );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertJob( jobStatusResult, "job-22", "IndexPopulationMain", "2020-06-24T18:00:00Z", "db 1", "user 1", "something very useful", "DELAYED",
                "2020-06-24T18:10:00Z", "", "SCHEDULED", "" );
    }

    @Test
    void testPeriodic()
    {
        var jobInfo = new MonitoredJobInfo( 33, INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), new Subject( "user 1" ), "db 1",
                "something very useful", Instant.parse( "2020-06-24T18:10:00Z" ), Duration.ofMillis( 182_001 ), SCHEDULED, JobType.PERIODIC, null );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertJob( jobStatusResult, "job-33", "IndexPopulationMain", "2020-06-24T18:00:00Z", "db 1", "user 1", "something very useful", "PERIODIC",
                "2020-06-24T18:10:00Z", "00:03:02.001", "SCHEDULED", "" );
    }

    void assertJob( JobStatusResult job, String id, String group, String submitted, String database, String submitter, String description, String type,
            String scheduledAt, String period, String state, String currentStateDescription )
    {
        assertEquals( id, job.jobId );
        assertEquals( group, job.group );
        assertEquals( submitted, job.submitted );
        assertEquals( database, job.database );
        assertEquals( submitter, job.submitter );
        assertEquals( description, job.description );
        assertEquals( type, job.type );
        assertEquals( scheduledAt, job.scheduledAt );
        assertEquals( period, job.period );
        assertEquals( state, job.state );
        assertEquals( currentStateDescription, job.currentStateDescription );
    }
}
