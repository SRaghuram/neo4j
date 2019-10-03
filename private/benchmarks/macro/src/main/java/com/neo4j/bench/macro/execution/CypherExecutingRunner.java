/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.profiling.InternalProfiler;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.database.Database;
import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.QueryString;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.neo4j.bench.common.tool.macro.ExecutionMode.EXECUTE;
import static com.neo4j.bench.common.tool.macro.ExecutionMode.PLAN;
import static com.neo4j.bench.macro.execution.measurement.MeasurementControl.single;

public class CypherExecutingRunner extends QueryRunner
{
    private final Function<ForkDirectory,Database> databaseCreator;

    CypherExecutingRunner( Function<ForkDirectory,Database> databaseCreator )
    {
        this.databaseCreator = databaseCreator;
    }

    @Override
    protected void run( Jvm jvm,
                        Map<Pid,Parameters> pidParameters,
                        Map<Pid,List<InternalProfiler>> pidProfilers,
                        Query query,
                        ForkDirectory forkDirectory,
                        MeasurementControl warmupControl,
                        MeasurementControl measurementControl )
    {
        try
        {
            WarmupStrategy warmupStrategy = warmupStrategyFor( query, warmupControl );
            try ( Database database = databaseCreator.apply( forkDirectory ) )
            {
                Runner.run( jvm,
                            database,
                            pidParameters,
                            pidProfilers,
                            warmupStrategy.warmupQuery(),
                            query.copyWith( EXECUTE ).queryString(),
                            query.benchmarkGroup(),
                            query.benchmark(),
                            query.parameters().create(),
                            forkDirectory,
                            warmupStrategy.warmupControl(),
                            query.isSingleShot() ? single() : measurementControl,
                            warmupStrategy.doRollbackOnWarmup() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error running query\n" + query.toString(), e );
        }
    }

    /*
      Warmup strategy preference:
      1. Rollback     : even when warmup query is available, if safe to rollback that is preferred as it means warmup is as close to measurement as possible.
      2. Warmup query : when not safe to rollback (e.g., with PERIODIC COMMIT) and warmup query is available, that is the best we can do.
                        * note, warmup queries are assumed to always be non-mutating <-- TODO move isMutating to QueryString
      3. PLAN         : when neither of the other options is available to us, fallback to EXPLAIN, it is the best we can do.
     ----------------------------------------------------------------------------------------------------------------------------------------------------------
     HAS_WARMUP IS_MUTATING PERIODIC_COMMIT IS_SINGLE_SHOT |  RUN_MODE DO_ROLL_BACK WARMUP_QUERY   | Reason
     ----------------------------------------------------------------------------------------------------------------------------------------------------------
     0          0           0               n/a            |  EXECUTE  false        MAIN           | not mutating                    > no need for rollback
     0          0           1               n/a            |  EXECUTE  false        MAIN           | not mutating                    > no need for rollback
     0          1           0               n/a            |  EXECUTE  true         MAIN           | mutating                        > rollback required
     0          1           1               n/a            |  PLAN     false        MAIN           | mutating & PERIODIC & no warmup > only option is PLAN
     1          0           0               n/a            |  EXECUTE  false        MAIN           | not mutating                    > no need for rollback
     1          0           1               n/a            |  EXECUTE  false        MAIN           | not mutating                    > no need for rollback
     1          1           0               n/a            |  EXECUTE  true         MAIN           | mutating                        > rollback required
     1          1           1               n/a            |  EXECUTE  false        WARMUP         | mutating & PERIODIC & warmup    > warmup query best option
     ----------------------------------------------------------------------------------------------------------------------------------------------------------
     */
    private static WarmupStrategy warmupStrategyFor( Query query, MeasurementControl warmupControl )
    {
        if ( !query.isMutating() )
        {
            // all non-mutating cases
            return new WarmupStrategy( false, query.copyWith( EXECUTE ).queryString(), warmupControl );
        }
        else
        {
            // mutating cases
            if ( !query.queryString().isPeriodicCommit() )
            {
                // mutating cases where main query does not contain PERIODIC COMMIT -> we can rollback
                return new WarmupStrategy( true, query.copyWith( EXECUTE ).queryString(), warmupControl );
            }
            else
            {
                // mutating cases where main query contains PERIODIC COMMIT -> not possible to rollback
                Optional<QueryString> maybeWarmupQuery = query.warmupQueryString();
                if ( maybeWarmupQuery.isPresent() )
                {
                    // when there is a (non-mutating) warmup query fallback to it
                    QueryString warmupQuery = maybeWarmupQuery.map( qs -> qs.copyWith( EXECUTE ) ).get();
                    return new WarmupStrategy( false, warmupQuery, warmupControl );
                }
                else
                {
                    // there is no (non-mutating) warmup query to fallback to -> only option is to PLAN, and only one execution is needed to cache the plan
                    return new WarmupStrategy( false, query.copyWith( PLAN ).queryString(), MeasurementControl.single() );
                }
            }
        }
    }

    private static class WarmupStrategy
    {
        private final boolean doRollbackOnWarmup;
        private final QueryString warmupQuery;
        private final MeasurementControl warmupControl;

        private WarmupStrategy( boolean doRollbackOnWarmup, QueryString warmupQuery, MeasurementControl warmupControl )
        {
            this.doRollbackOnWarmup = doRollbackOnWarmup;
            this.warmupQuery = warmupQuery;
            this.warmupControl = warmupControl;
        }

        private boolean doRollbackOnWarmup()
        {
            return doRollbackOnWarmup;
        }

        private QueryString warmupQuery()
        {
            return warmupQuery;
        }

        MeasurementControl warmupControl()
        {
            return warmupControl;
        }
    }
}
