package com.neo4j.bench.macro.workload;

import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.options.Runtime;
import com.neo4j.bench.macro.execution.Options;

public class StaticQueryString extends QueryString
{
    static StaticQueryString atDefaults( String value )
    {
        return new StaticQueryString( Planner.DEFAULT, Runtime.DEFAULT, Options.ExecutionMode.EXECUTE, value );
    }

    private final String value;

    private StaticQueryString(
            Planner planner,
            Runtime runtime,
            Options.ExecutionMode executionMode,
            String value )
    {
        super( planner, runtime, executionMode );
        this.value = value;
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
    public QueryString copyWith( Options.ExecutionMode newExecutionMode )
    {
        return new StaticQueryString( this.planner(), this.runtime(), newExecutionMode, value );
    }

    @Override
    public boolean isPeriodicCommit()
    {
        return value.toLowerCase().contains( "periodic commit" );
    }
}
