/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.macro.execution.Options;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class QueryStringTest
{
    private static final String rawQueryString = "RETURN 1";

    @Test
    public void shouldKnowIfItIsPeriodicCommit()
    {
        QueryString periodicQS = StaticQueryString.atDefaults( "USING PERIODIC COMMIT 500\n" +
                                                               "LOAD CSV FROM 'https://neo4j.com/docs/developer-manual/3.4/csv/artists.csv' AS line\n" +
                                                               "CREATE (:Artist { name: line[1], year: toInteger(line[2])})" );
        assertTrue( periodicQS.isPeriodicCommit() );
    }

    @Test
    public void shouldKnowIfItIsNotPeriodicCommit()
    {
        QueryString nonPeriodicQS = StaticQueryString.atDefaults( "LOAD CSV FROM 'https://neo4j.com/docs/developer-manual/3.4/csv/artists.csv' AS line\n" +
                                                                  "CREATE (:Artist { name: line[1], year: toInteger(line[2])})" );
        assertFalse( nonPeriodicQS.isPeriodicCommit() );
    }

    @Test
    public void changingQueryStringWithChangingSuppliedShouldActuallyBeChanging()
    {
        QueryString changingQS = ChangingQueryString.atDefaults(
                () -> String.format( "MATCH (n:N) WHERE n.`%s` > 0 RETURN n", UUID.randomUUID().toString() ) );
        assertNotEquals( changingQS.value(), changingQS.value() );
    }

    @Test
    public void shouldFormatRawValueWithDefaults()
    {
        QueryString qs = StaticQueryString.atDefaults( rawQueryString );
        assertEquals( rawQueryString, qs.value() );
    }

    @Test
    public void shouldFormatRawValueWithPlan()
    {
        QueryString qs = StaticQueryString.atDefaults( rawQueryString )
                                          .copyWith( Planner.COST );
        assertEquals( "CYPHER planner=cost " + rawQueryString, qs.value() );
    }

    @Test
    public void shouldFormatRawValueWithRuntime()
    {
        QueryString qs = StaticQueryString.atDefaults( rawQueryString )
                                          .copyWith( Runtime.SLOTTED );
        assertEquals( "CYPHER runtime=slotted " + rawQueryString, qs.value() );
    }

    @Test
    public void shouldFormatRawValueWithExecutionMode()
    {
        QueryString qs = StaticQueryString.atDefaults( rawQueryString )
                                          .copyWith( Options.ExecutionMode.PROFILE );
        assertEquals( "CYPHER PROFILE " + rawQueryString, qs.value() );
    }

    @Test
    public void shouldFormatRawValueWithEverything()
    {
        QueryString qs = StaticQueryString.atDefaults( rawQueryString )
                                          .copyWith( Planner.COST )
                                          .copyWith( Runtime.SLOTTED )
                                          .copyWith( Options.ExecutionMode.PROFILE );
        assertThat( qs.value(), anyOf( equalTo( "CYPHER runtime=slotted planner=cost PROFILE " + rawQueryString ),
                                       equalTo( "CYPHER planner=cost runtime=slotted PROFILE " + rawQueryString ) ) );
    }
}
