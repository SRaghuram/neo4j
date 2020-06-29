/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobType;
import org.neo4j.scheduler.MonitoredJobInfo;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JobStatusResultTest
{
    @Test
    void testImmediateJobWithDatabaseAndUser()
    {
        var jobInfo =
                new MonitoredJobInfo( Group.INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), "user 1", "db 1", "something very useful", null, null,
                        MonitoredJobInfo.State.SCHEDULED, JobType.IMMEDIATE );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertEquals( "IndexPopulationMain 2020-06-24T18:00:00Z db 1 user 1 something very useful IMMEDIATE   SCHEDULED", resultToString( jobStatusResult ) );
    }

    @Test
    void testImmediateJobWithoutDatabaseAndUser()
    {
        var jobInfo = new MonitoredJobInfo( Group.INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), null, null, "something very useful", null, null,
                MonitoredJobInfo.State.EXECUTING, JobType.IMMEDIATE );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertEquals( "IndexPopulationMain 2020-06-24T18:00:00Z   something very useful IMMEDIATE   EXECUTING", resultToString( jobStatusResult ) );
    }

    @Test
    void testDelayed()
    {
        var jobInfo = new MonitoredJobInfo( Group.INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), "user 1", "db 1", "something very useful",
                Instant.parse( "2020-06-24T18:10:00Z" ), null,
                MonitoredJobInfo.State.SCHEDULED, JobType.DELAYED );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertEquals( "IndexPopulationMain 2020-06-24T18:00:00Z db 1 user 1 something very useful DELAYED 2020-06-24T18:10:00Z  SCHEDULED",
                resultToString( jobStatusResult ) );
    }

    @Test
    void testPeriodic()
    {
        var jobInfo = new MonitoredJobInfo( Group.INDEX_POPULATION, Instant.parse( "2020-06-24T18:00:00Z" ), "user 1", "db 1", "something very useful",
                Instant.parse( "2020-06-24T18:10:00Z" ),
                Duration.ofMillis( 182_001 ),
                MonitoredJobInfo.State.SCHEDULED, JobType.PERIODIC );
        var jobStatusResult = new JobStatusResult( jobInfo, UTC );
        assertEquals( "IndexPopulationMain 2020-06-24T18:00:00Z db 1 user 1 something very useful PERIODIC 2020-06-24T18:10:00Z 00:03:02.001 SCHEDULED",
                resultToString( jobStatusResult ) );
    }

    String resultToString( JobStatusResult job )
    {
        return job.group + " " + job.submitted + " " + job.database + " " + job.submitter + " " + job.description + " "
                + job.type + " " + job.scheduledAt + " " + job.period + " " + job.state;
    }
}
