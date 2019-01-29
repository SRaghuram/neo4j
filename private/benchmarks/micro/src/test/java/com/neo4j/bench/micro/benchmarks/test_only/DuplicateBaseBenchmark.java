package com.neo4j.bench.micro.benchmarks.test_only;

import com.neo4j.bench.micro.benchmarks.core.AbstractCoreBenchmark;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import org.openjdk.jmh.annotations.Param;

@BenchmarkEnabled( true )
public class DuplicateBaseBenchmark extends AbstractCoreBenchmark
{
    @ParamValues(
            allowed = {"standard"},
            base = {"standard", "standard"} )
    @Param( {} )
    public String DuplicateBaseBenchmark_param;

    @Override
    public String benchmarkGroup()
    {
        return "Example";
    }

    @Override
    public String description()
    {
        return getClass().getSimpleName();
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }
}
