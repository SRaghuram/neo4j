/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.macro.workload.Parameters;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.QueryString;
import com.neo4j.bench.macro.workload.StaticQueryString;
import org.hamcrest.Description;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static com.neo4j.bench.common.tool.macro.ExecutionMode.PLAN;
import static com.neo4j.bench.macro.execution.WarmupStrategy.warmupStrategyFor;
import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.ofCount;
import static com.neo4j.bench.macro.workload.StaticQueryString.atDefaults;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@TestDirectoryExtension
public class WarmupStrategyTest
{
    @Test
    public void shouldFailToLoadWhenNoHeader()
    {
        // HAS_WARMUP IS_MUTATING PERIODIC_COMMIT IS_SINGLE_SHOT | RUN_MODE DO_ROLL_BACK WARMUP_QUERY | Reason
        // -----------------------------------------------------------------------------------------------------------------------------------------------------
        // -          0           0               -              | EXEC     false        MAIN         | not mutating                    > no need for rollback
        assertThat( warmupStrategyFor( QueryBuilder.init( "query" )
                                                   .build(),
                                       ofCount( 1 ) ),
                    equalTo( new WarmupStrategy( false, atDefaults( "query" ), ofCount( 1 ) ) ) );

        // HAS_WARMUP IS_MUTATING PERIODIC_COMMIT IS_SINGLE_SHOT | RUN_MODE DO_ROLL_BACK WARMUP_QUERY | Reason
        // -----------------------------------------------------------------------------------------------------------------------------------------------------
        // -          1           0               -              | EXEC     true         MAIN         | mutating                        > rollback required
        assertThat( warmupStrategyFor( QueryBuilder.init( "query" )
                                                   .isMutating()
                                                   .build(),
                                       ofCount( 1 ) ),
                    equalTo( new WarmupStrategy( true, atDefaults( "query" ), ofCount( 1 ) ) ) );

        // HAS_WARMUP IS_MUTATING PERIODIC_COMMIT IS_SINGLE_SHOT | RUN_MODE DO_ROLL_BACK WARMUP_QUERY | Reason
        // -----------------------------------------------------------------------------------------------------------------------------------------------------
        // 0          0           1               -              | EXEC     false        MAIN         | not mutating                    > no need for rollback
        assertThat( warmupStrategyFor( QueryBuilder.init( "periodic commit" )
                                                   .build(),
                                       ofCount( 1 ) ),
                    equalTo( new WarmupStrategy( false, atDefaults( "periodic commit" ), ofCount( 1 ) ) ) );

        // HAS_WARMUP IS_MUTATING PERIODIC_COMMIT IS_SINGLE_SHOT | RUN_MODE DO_ROLL_BACK WARMUP_QUERY | Reason
        // -----------------------------------------------------------------------------------------------------------------------------------------------------
        // 0          1           1               -              | PLAN     false        MAIN         | mutating & PERIODIC & no warmup > only option is PLAN
        assertThat( warmupStrategyFor( QueryBuilder.init( "periodic commit" ).isMutating()
                                                   .build(),
                                       ofCount( 1 ) ),
                    equalTo( new WarmupStrategy( false, atDefaults( "periodic commit" ).copyWith( PLAN ), ofCount( 1 ) ) ) );

        // HAS_WARMUP IS_MUTATING PERIODIC_COMMIT IS_SINGLE_SHOT | RUN_MODE DO_ROLL_BACK WARMUP_QUERY | Reason
        // -----------------------------------------------------------------------------------------------------------------------------------------------------
        // 1          0           1               -              | EXEC     false        MAIN         | not mutating                    > no need for rollback
        assertThat( warmupStrategyFor( QueryBuilder.init( "periodic commit" )
                                                   .warmup( "warmup" )
                                                   .build(),
                                       ofCount( 1 ) ),
                    equalTo( new WarmupStrategy( false, atDefaults( "periodic commit" ), ofCount( 1 ) ) ) );

        // HAS_WARMUP IS_MUTATING PERIODIC_COMMIT IS_SINGLE_SHOT | RUN_MODE DO_ROLL_BACK WARMUP_QUERY | Reason
        // -----------------------------------------------------------------------------------------------------------------------------------------------------
        // 1          1           1               -              | PLAN     false        MAIN         | mutating & PERIODIC & warmup    > PLAN + warmup query
        assertThat( warmupStrategyFor( QueryBuilder.init( "periodic commit" )
                                                   .isMutating()
                                                   .warmup( "warmup" )
                                                   .build(),
                                       ofCount( 1 ) ),
                    equalTo( new WarmupStrategy( false, atDefaults( "periodic commit" ).copyWith( PLAN ), ofCount( 1 ) ) ) );
    }

    private static class QueryBuilder
    {
        private boolean isMutating;
        private Optional<QueryString> maybeWarmupQueryString;
        private final QueryString queryString;

        private static QueryBuilder init( String queryString )
        {
            return new QueryBuilder( atDefaults( queryString ) );
        }

        private QueryBuilder( QueryString queryString )
        {
            this.queryString = queryString;
            this.isMutating = false;
            this.maybeWarmupQueryString = Optional.empty();
        }

        private QueryBuilder isMutating()
        {
            this.isMutating = true;
            return this;
        }

        private QueryBuilder warmup( String queryString )
        {
            this.maybeWarmupQueryString = Optional.of( StaticQueryString.atDefaults( queryString ) );
            return this;
        }

        private Query build()
        {
            return new Query( "group",
                              "name",
                              "description",
                              maybeWarmupQueryString,
                              queryString,
                              false /*is single shot*/,
                              isMutating,
                              Parameters.empty(),
                              Deployment.embedded() );
        }
    }
}
