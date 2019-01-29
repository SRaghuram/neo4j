package com.neo4j.bench.macro.workload;

import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.macro.execution.Options;

import java.util.function.Supplier;

public class ChangingQueryString extends QueryString
{
    static ChangingQueryString atDefaults( Supplier<String> values )
    {
        return new ChangingQueryString( Planner.DEFAULT, Runtime.DEFAULT, Options.ExecutionMode.EXECUTE, values, false );
    }

    private final Supplier<String> values;

    private final boolean isPeriodicCommit;

    private ChangingQueryString(
            Planner planner,
            Runtime runtime,
            Options.ExecutionMode executionMode,
            Supplier<String> values, boolean isPeriodicCommit )
    {
        super( planner, runtime, executionMode );
        this.values = values;
        this.isPeriodicCommit = isPeriodicCommit;
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
    public QueryString copyWith( Options.ExecutionMode newExecutionMode )
    {
        return new ChangingQueryString( this.planner(), this.runtime(), newExecutionMode, values, isPeriodicCommit );
    }

    @Override
    public boolean isPeriodicCommit()
    {
        return isPeriodicCommit;
    }
}
