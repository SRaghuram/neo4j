/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.macro.execution.Options;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryTest
{
    private static final QueryString queryString = StaticQueryString.atDefaults( "RETURN 1" );
    private static final Query baseQuery = new Query(
            "g",
            "n",
            "d",
            queryString,
            queryString,
            true,
            true,
            false,
            Parameters.empty()
    );

    @Test
    public void copyWithPlannerShouldChangePlanner()
    {
        Query copiedQuery = baseQuery.copyWith( Planner.COST );
        assertEquals( Planner.COST, copiedQuery.queryString().planner() );
        assertEquals( Planner.COST, copiedQuery.warmupQueryString().planner() );
    }

    @Test
    public void copyWithRuntimeShouldChangePlanner()
    {
        Query copiedQuery = baseQuery.copyWith( Runtime.SLOTTED );
        assertEquals( Runtime.SLOTTED, copiedQuery.queryString().runtime() );
        assertEquals( Runtime.SLOTTED, copiedQuery.warmupQueryString().runtime() );
    }

    @Test
    public void copyWithExecutionModeShouldChangePlanner()
    {
        Query copiedQuery = baseQuery.copyWith( Options.ExecutionMode.EXECUTE );
        assertEquals( Options.ExecutionMode.EXECUTE, copiedQuery.queryString().executionMode() );
        assertEquals( Options.ExecutionMode.EXECUTE, copiedQuery.warmupQueryString().executionMode() );
    }
}
