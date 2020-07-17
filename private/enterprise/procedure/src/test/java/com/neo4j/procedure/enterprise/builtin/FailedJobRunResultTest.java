/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneOffset;

import org.neo4j.common.Subject;
import org.neo4j.scheduler.FailedJobRun;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobType;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FailedJobRunResultTest
{
    @Test
    void testFailedJobRun()
    {
        var jobRun = new FailedJobRun( 99,
                Group.INDEX_POPULATION,
                new Subject( "user 1" ),
                "db 1",
                "a very useful job",
                JobType.IMMEDIATE,
                Instant.parse( "2020-06-24T18:00:00Z" ),
                Instant.parse( "2020-06-24T18:10:00Z" ),
                Instant.parse( "2020-06-24T18:20:00Z" ),
                new IllegalStateException( "Something went terribly wrong" ) );
        var jobRunResult = new FailedJobRunResult( jobRun, ZoneOffset.UTC );

        assertEquals( "job-99", jobRunResult.jobId );
        assertEquals( "IndexPopulationMain", jobRunResult.group );
        assertEquals( "user 1", jobRunResult.submitter );
        assertEquals( "db 1", jobRunResult.database );
        assertEquals( "a very useful job", jobRunResult.description );
        assertEquals( "IMMEDIATE", jobRunResult.type );
        assertEquals( "2020-06-24T18:00:00Z", jobRunResult.submitted );
        assertEquals( "2020-06-24T18:10:00Z", jobRunResult.executionStart );
        assertEquals( "2020-06-24T18:20:00Z", jobRunResult.failureTime );
        assertEquals( "IllegalStateException: Something went terribly wrong", jobRunResult.failureDescription );
    }
}
