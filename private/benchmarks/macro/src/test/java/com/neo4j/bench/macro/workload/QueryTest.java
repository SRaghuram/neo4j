/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryTest
{
    private static final QueryString queryString = StaticQueryString.atDefaults( "RETURN 1" );
    private static final Query baseQuery = new Query(
            "g",
            "n",
            "d",
            Optional.of( queryString ),
            queryString,
            true,
            false,
            Parameters.empty(),
            Deployment.embedded() );

    @Test
    void copyWithPlannerShouldChangePlanner()
    {
        Query copiedQuery = baseQuery.copyWith( Planner.COST );
        assertEquals( Planner.COST, copiedQuery.queryString().planner() );
        assertEquals( Planner.COST, copiedQuery.warmupQueryString().orElseThrow( RuntimeException::new ).planner() );
    }

    @Test
    void copyWithRuntimeShouldChangePlanner()
    {
        Query copiedQuery = baseQuery.copyWith( Runtime.SLOTTED );
        assertEquals( Runtime.SLOTTED, copiedQuery.queryString().runtime() );
        assertEquals( Runtime.SLOTTED, copiedQuery.warmupQueryString().orElseThrow( RuntimeException::new ).runtime() );
    }

    @Test
    void copyWithExecutionModeShouldChangePlanner()
    {
        Query copiedQuery = baseQuery.copyWith( ExecutionMode.EXECUTE );
        assertEquals( ExecutionMode.EXECUTE, copiedQuery.queryString().executionMode() );
        assertEquals( ExecutionMode.EXECUTE, copiedQuery.warmupQueryString().orElseThrow( RuntimeException::new ).executionMode() );
    }
}
