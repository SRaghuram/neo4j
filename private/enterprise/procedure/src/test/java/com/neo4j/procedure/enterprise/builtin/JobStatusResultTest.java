/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
        assertEquals( "job-99 IndexPopulationMain 2020-06-24T18:00:00Z db 1 user 1 something very useful IMMEDIATE   SCHEDULED Pretty impressive progress",
                resultToString( jobStatusResult ) );
    }

    @Test
    void testImmediateJobWithoutDatabaseAndUser()
    {
        var jobInfo = new MonitoredJobInfo( 11, INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), SYSTEM, null, "something very useful", null, null,
                MonitoredJobInfo.State.EXECUTING, IMMEDIATE, null );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertEquals( "job-11 IndexPopulationMain 2020-06-24T18:00:00Z   something very useful IMMEDIATE   EXECUTING ", resultToString( jobStatusResult ) );
    }

    @Test
    void testDelayed()
    {
        var jobInfo = new MonitoredJobInfo( 22, INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), new Subject( "user 1" ), "db 1",
                "something very useful", Instant.parse( "2020-06-24T18:10:00Z" ), null, SCHEDULED, JobType.DELAYED, null  );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertEquals( "job-22 IndexPopulationMain 2020-06-24T18:00:00Z db 1 user 1 something very useful DELAYED 2020-06-24T18:10:00Z  SCHEDULED ",
                resultToString( jobStatusResult ) );
    }

    @Test
    void testPeriodic()
    {
        var jobInfo = new MonitoredJobInfo( 33, INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), new Subject( "user 1" ), "db 1",
                "something very useful", Instant.parse( "2020-06-24T18:10:00Z" ), Duration.ofMillis( 182_001 ), SCHEDULED, JobType.PERIODIC, null );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertEquals( "job-33 IndexPopulationMain 2020-06-24T18:00:00Z db 1 user 1 something very useful PERIODIC 2020-06-24T18:10:00Z 00:03:02.001 SCHEDULED ",
                resultToString( jobStatusResult ) );
    }

    String resultToString( JobStatusResult job )
    {
        return job.jobId + " " + job.group + " " + job.submitted + " " + job.database + " " + job.submitter + " " + job.description + " "
                + job.type + " " + job.scheduledAt + " " + job.period + " " + job.state + " " + job.currentStateDescription;
    }
}
