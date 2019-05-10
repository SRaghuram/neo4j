package com.neo4j.bench.jmh.api;

import com.neo4j.bench.jmh.api.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.infra.Blackhole;

import java.util.OptionalInt;
import java.util.stream.IntStream;

public class SimpleBenchmark extends BaseBenchmark
{

    @ParamValues(
            allowed = {"10000", "20000"},
            base = {"10000", "20000"}
    )
    @Param( {} )
    public Integer SimpleBenchmark_range;

    @Benchmark
    @BenchmarkMode( Mode.AverageTime )
    public void myBenchmark( Blackhole bh )
    {
        OptionalInt reduce = IntStream.range( 1, SimpleBenchmark_range ).reduce( ( i, x ) -> (x + (i * (i - 1))) );
        bh.consume( reduce.orElse( 0 ) ) ;
    }

    @Override
    public String description()
    {
        return "simple";
    }

    @Override
    public String benchmarkGroup()
    {
        return "test";
    }

    @Override
    public boolean isThreadSafe()
    {
        return true;
    }
}
