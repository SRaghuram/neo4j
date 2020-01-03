/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.tool.macro.ExecutionMode;

public class StaticQueryString extends QueryString
{
    static StaticQueryString atDefaults( String value )
    {
        return new StaticQueryString( Planner.DEFAULT, Runtime.DEFAULT, ExecutionMode.EXECUTE, value );
    }

    private final String value;

    private StaticQueryString(
            Planner planner,
            Runtime runtime,
            ExecutionMode executionMode,
            String value )
    {
        super( planner, runtime, executionMode );
        this.value = value;
    }

    @Override
    protected String stableValue()
    {
        return value;
    }

    @Override
    protected String rawValue()
    {
        return value;
    }

    @Override
    public QueryString copyWith( Runtime newRuntime )
    {
        return new StaticQueryString( this.planner(), newRuntime, this.executionMode(), value );
    }

    @Override
    public QueryString copyWith( Planner newPlanner )
    {
        return new StaticQueryString( newPlanner, this.runtime(), this.executionMode(), value );
    }

    @Override
    public QueryString copyWith( ExecutionMode newExecutionMode )
    {
        return new StaticQueryString( this.planner(), this.runtime(), newExecutionMode, value );
    }

    @Override
    public boolean isPeriodicCommit()
    {
        return value.toLowerCase().contains( "periodic commit" );
    }
}
