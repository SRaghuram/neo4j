/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        ChangingQueryString.ValueSupplier valueSupplier = new ChangingQueryString.ValueSupplier()
        {
            @Override
            public String stableTemplate()
            {
                return "MATCH (n:N) WHERE n.`%s` > 0 RETURN n";
            }

            @Override
            public String get()
            {
                return String.format( stableTemplate(), UUID.randomUUID().toString() );
            }
        };
        QueryString changingQS = ChangingQueryString.atDefaults( valueSupplier );
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
                                          .copyWith( ExecutionMode.CARDINALITY );
        assertEquals( "CYPHER PROFILE " + rawQueryString, qs.value() );
    }

    @Test
    public void shouldFormatRawValueWithEverything()
    {
        QueryString qs = StaticQueryString.atDefaults( rawQueryString )
                                          .copyWith( Planner.COST )
                                          .copyWith( Runtime.SLOTTED )
                                          .copyWith( ExecutionMode.CARDINALITY );
        assertThat( qs.value(), anyOf( equalTo( "CYPHER runtime=slotted planner=cost PROFILE " + rawQueryString ),
                                       equalTo( "CYPHER planner=cost runtime=slotted PROFILE " + rawQueryString ) ) );
    }
}
