/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.tool.macro.ExecutionMode;

import java.util.function.Supplier;

public class ChangingQueryString extends QueryString
{
    interface ValueSupplier extends Supplier<String>
    {
        String stableTemplate();
    }

    static ChangingQueryString atDefaults( ValueSupplier values )
    {
        return new ChangingQueryString( Planner.DEFAULT, Runtime.DEFAULT, ExecutionMode.EXECUTE, values, false );
    }

    private final ValueSupplier values;

    private final boolean isPeriodicCommit;

    private ChangingQueryString(
            Planner planner,
            Runtime runtime,
            ExecutionMode executionMode,
            ValueSupplier values,
            boolean isPeriodicCommit )
    {
        super( planner, runtime, executionMode );
        this.values = values;
        this.isPeriodicCommit = isPeriodicCommit;
    }

    @Override
    protected String stableValue()
    {
        return values.stableTemplate();
    }

    @Override
    protected String rawValue()
    {
        return values.get();
    }

    @Override
    public QueryString copyWith( Runtime newRuntime )
    {
        return new ChangingQueryString( this.planner(), newRuntime, this.executionMode(), values, isPeriodicCommit );
    }

    @Override
    public QueryString copyWith( Planner newPlanner )
    {
        return new ChangingQueryString( newPlanner, this.runtime(), this.executionMode(), values, isPeriodicCommit );
    }

    @Override
    public QueryString copyWith( ExecutionMode newExecutionMode )
    {
        return new ChangingQueryString( this.planner(), this.runtime(), newExecutionMode, values, isPeriodicCommit );
    }

    @Override
    public boolean isPeriodicCommit()
    {
        return isPeriodicCommit;
    }
}
