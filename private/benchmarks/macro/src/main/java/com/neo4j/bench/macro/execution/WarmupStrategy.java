/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.neo4j.bench.macro.execution.measurement.MeasurementControl;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.QueryString;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static com.neo4j.bench.common.tool.macro.ExecutionMode.EXECUTE;
import static com.neo4j.bench.common.tool.macro.ExecutionMode.PLAN;

class WarmupStrategy
{
    /*
    Warmup strategy preference:
    1. Rollback      : even if warmup query is available, if safe to rollback that is preferred as it means warmup is as closer to measurement.
    2. Warmup + PLAN : when not safe to rollback (PERIODIC COMMIT) and warmup query is available, EXPLAIN measurement query & execute warmup query.
                       * EXPLAIN forces code generation to occur during warmup phase
                       * warmup query performs additional warmup, e.g., pulls data into the page cache
                       * NOTE: warmup queries are assumed to always be non-mutating <-- TODO move isMutating to QueryString
                       * NOTE: only PERIODIC COMMIT queries can have a warmup query
    3. PLAN          : when neither of the other options is available to us, fallback to EXPLAIN, it is the best we can do.

    -------------------------------------------------------------------------------------------------------------------------------------------------------
    HAS_WARMUP IS_MUTATING PERIODIC_COMMIT IS_SINGLE_SHOT | RUN_MODE DO_ROLL_BACK WARMUP_QUERY | Reason
    -------------------------------------------------------------------------------------------------------------------------------------------------------
    -          0           0               -              | EXEC     false        MAIN         | not mutating                    > no need for rollback
    0          0           1               -              | EXEC     false        MAIN         | not mutating                    > no need for rollback
    -          1           0               -              | EXEC     true         MAIN         | mutating                        > rollback required
    0          1           1               -              | PLAN     false        MAIN         | mutating & PERIODIC & no warmup > only option is PLAN
    -          0           0               -              | ----     -----        ----         | <duplicate>
    1          0           1               -              | EXEC     false        MAIN         | not mutating                    > no need for rollback
    -          1           0               -              | ----     -----        ----         | <duplicate>
    1          1           1               -              | PLAN     false        MAIN         | mutating & PERIODIC & warmup    > PLAN + warmup query
    -------------------------------------------------------------------------------------------------------------------------------------------------------
    */
    static WarmupStrategy warmupStrategyFor( Query query, MeasurementControl warmupControl )
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
                if ( query.warmupQueryString().isPresent() )
                {
                    // TODO we should both PLAN & run warmup query in here, for now prefer PLAN of measurement query over execute of warmup query
                    // when there is a (non-mutating) warmup query fallback to it
//                    return new WarmupStrategy( false, warmupQuery.copyWith( EXECUTE ), warmupControl );
                    return new WarmupStrategy( false, query.copyWith( PLAN ).queryString(), warmupControl );
                }
                else
                {
                    // there is no (non-mutating) warmup query to fallback to -> only option is to PLAN,
                    // use same warmup control: PLAN as many times as possible within warmup, to both cache plan and try to trigger (JIT) code generation
                    return new WarmupStrategy( false, query.copyWith( PLAN ).queryString(), warmupControl );
                }
            }
        }
    }

    private final boolean doRollbackOnWarmup;
    private final QueryString warmupQuery;
    private final MeasurementControl warmupControl;

    WarmupStrategy( boolean doRollbackOnWarmup, QueryString warmupQuery, MeasurementControl warmupControl )
    {
        this.doRollbackOnWarmup = doRollbackOnWarmup;
        this.warmupQuery = warmupQuery;
        this.warmupControl = warmupControl;
    }

    boolean doRollbackOnWarmup()
    {
        return doRollbackOnWarmup;
    }

    QueryString warmupQuery()
    {
        return warmupQuery;
    }

    MeasurementControl warmupControl()
    {
        return warmupControl;
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public boolean equals( Object obj )
    {
        return EqualsBuilder.reflectionEquals( this, obj );
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SIMPLE_STYLE );
    }
}
