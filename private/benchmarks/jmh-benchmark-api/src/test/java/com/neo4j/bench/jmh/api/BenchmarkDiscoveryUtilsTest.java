package com.neo4j.bench.jmh.api;

import com.neo4j.bench.jmh.api.benchmarks.valid.ValidDisabledBenchmark;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark1;
import com.neo4j.bench.jmh.api.benchmarks.valid.ValidEnabledBenchmark2;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BenchmarkDiscoveryUtilsTest
{
    @Test
    public void shouldReadDescription()
    {
        String description = BenchmarkDiscoveryUtils.descriptionFor( ValidEnabledBenchmark1.class );

        assertThat( description, equalTo( ValidEnabledBenchmark1.class.getSimpleName() ) );
    }

    @Test
    public void shouldReadIsEnabled()
    {
        boolean benchmark1IsEnabled = BenchmarkDiscoveryUtils.isEnabled( ValidEnabledBenchmark1.class );
        boolean benchmark2IsEnabled = BenchmarkDiscoveryUtils.isEnabled( ValidEnabledBenchmark2.class );
        boolean benchmark3IsEnabled = BenchmarkDiscoveryUtils.isEnabled( ValidDisabledBenchmark.class );

        assertTrue( benchmark1IsEnabled );
        assertTrue( benchmark2IsEnabled );
        assertFalse( benchmark3IsEnabled );
    }

    @Test
    public void shouldReadBenchmarkGroup()
    {
        String group = BenchmarkDiscoveryUtils.benchmarkGroupFor( ValidEnabledBenchmark1.class );

        assertThat( group, equalTo( "Example" ) );
    }

    @Test
    public void shouldReadBenchmarkIsThreadSafe()
    {
        boolean benchmark1IsThreadSafe = BenchmarkDiscoveryUtils.isThreadSafe( ValidEnabledBenchmark1.class );
        boolean benchmark2IsThreadSafe = BenchmarkDiscoveryUtils.isThreadSafe( ValidEnabledBenchmark2.class );

        assertFalse( benchmark1IsThreadSafe );
        assertTrue( benchmark2IsThreadSafe );
    }
}
