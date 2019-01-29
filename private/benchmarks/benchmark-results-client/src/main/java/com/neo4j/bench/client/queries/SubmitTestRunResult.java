package com.neo4j.bench.client.queries;

import com.neo4j.bench.client.model.BenchmarkMetrics;
import com.neo4j.bench.client.model.TestRun;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SubmitTestRunResult
{
    private final TestRun testRun;
    private final List<BenchmarkMetrics> benchmarkMetricsLst;

    public SubmitTestRunResult( TestRun testRun, List<BenchmarkMetrics> benchmarkMetricsList )
    {
        this.testRun = requireNonNull( testRun );
        this.benchmarkMetricsLst = requireNonNull( benchmarkMetricsList );
    }

    public TestRun testRun()
    {
        return testRun;
    }

    public List<BenchmarkMetrics> benchmarkMetricsList()
    {
        return benchmarkMetricsLst;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        SubmitTestRunResult that = (SubmitTestRunResult) o;
        return Objects.equals( testRun, that.testRun ) &&
               Objects.equals( benchmarkMetricsLst, that.benchmarkMetricsLst );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( testRun, benchmarkMetricsLst );
    }

    @Override
    public String toString()
    {
        return "SubmitTestRunResult{" +
               "testRun=" + testRun +
               ", benchmarkMetricsList=" + benchmarkMetricsLst +
               '}';
    }
}
