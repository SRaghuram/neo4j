/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class BenchmarkGroupBenchmarkMetrics
{
    private final Map<BenchmarkGroup,Map<Benchmark,AnnotatedMetrics>> inner = new HashMap<>();

    public void addAll( BenchmarkGroupBenchmarkMetrics other )
    {
        requireNonNull( other );
        for ( BenchmarkGroup group : other.inner.keySet() )
        {
            Map<Benchmark,AnnotatedMetrics> annotatedBenchmarks = other.inner.get( group );
            for ( Benchmark benchmark : annotatedBenchmarks.keySet() )
            {
                AnnotatedMetrics annotatedMetrics = annotatedBenchmarks.get( benchmark );
                add( group, benchmark, annotatedMetrics.metrics(), annotatedMetrics.neo4jConfig() );
            }
        }
    }

    public void add( BenchmarkGroup group, Benchmark benchmark, Metrics metrics, Neo4jConfig neo4jConfig )
    {
        requireNonNull( group );
        requireNonNull( benchmark );
        requireNonNull( metrics );
        requireNonNull( neo4jConfig );

        Map<Benchmark,AnnotatedMetrics> benchmarkMetrics = inner.computeIfAbsent( group, key -> new HashMap<>() );
        if ( benchmarkMetrics.containsKey( benchmark ) )
        {
            throw new IllegalStateException( format( "Multiple results for benchmark: %s\nExisting: %s\nNew: %s",
                                                     benchmark,
                                                     benchmarkMetrics.get( benchmark ).metrics(),
                                                     metrics ) );
        }
        benchmarkMetrics.put( benchmark, new AnnotatedMetrics( metrics, neo4jConfig ) );
    }

    List<BenchmarkGroupBenchmark> benchmarkGroupBenchmarks()
    {
        return inner.keySet().stream()
                    .flatMap( group -> inner.get( group ).keySet().stream()
                                            .map( benchmark -> new BenchmarkGroupBenchmark( group, benchmark ) ) )
                    .collect( Collectors.toList() );
    }

    public void attachProfilerRecording( BenchmarkGroup group, Benchmark benchmark, ProfilerRecordings profilerRecordings )
    {
        inner.get( group ).get( benchmark ).attachProfilesRecording( profilerRecordings );
    }

    public AnnotatedMetrics getMetricsFor( BenchmarkGroupBenchmark benchmarkGroupBenchmark )
    {
        return getMetricsFor( benchmarkGroupBenchmark.benchmarkGroup(), benchmarkGroupBenchmark.benchmark() );
    }

    public AnnotatedMetrics getMetricsFor( BenchmarkGroup group, Benchmark benchmark )
    {
        return inner.get( group ).get( benchmark );
    }

    public List<List<Object>> toList()
    {
        return toRows().stream().map( Row::toList ).collect( Collectors.toList() );
    }

    private List<Row> toRows()
    {
        List<Row> list = new ArrayList<>();
        for ( Map.Entry<BenchmarkGroup,Map<Benchmark,AnnotatedMetrics>> groupEntry : inner.entrySet() )
        {
            BenchmarkGroup group = groupEntry.getKey();
            for ( Map.Entry<Benchmark,AnnotatedMetrics> benchmarkMetrics : groupEntry.getValue().entrySet() )
            {
                Benchmark benchmark = benchmarkMetrics.getKey();
                AnnotatedMetrics annotatedMetrics = benchmarkMetrics.getValue();
                list.add( new Row(
                        group,
                        benchmark,
                        annotatedMetrics.metrics(),
                        annotatedMetrics.neo4jConfig(),
                        annotatedMetrics.profilerRecordings() ) );
            }
        }
        return list;
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
        BenchmarkGroupBenchmarkMetrics that = (BenchmarkGroupBenchmarkMetrics) o;
        return Objects.equals( inner, that.inner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( inner );
    }

    @Override
    public String toString()
    {
        return toRows().stream().map( row -> row.toString() + "\n" ).collect( joining() );
    }

    public String errorDetails()
    {
        List<Entry> entries = new ArrayList<>();
        int maxBenchmarkLength = Integer.MIN_VALUE;
        for ( Map.Entry<BenchmarkGroup,Map<Benchmark,AnnotatedMetrics>> groupEntry : inner.entrySet() )
        {
            for ( Map.Entry<Benchmark,AnnotatedMetrics> benchmarkMetrics : groupEntry.getValue().entrySet() )
            {
                Benchmark benchmark = benchmarkMetrics.getKey();
                AnnotatedMetrics annotatedMetrics = benchmarkMetrics.getValue();
                maxBenchmarkLength = Math.max( maxBenchmarkLength, benchmark.name().length() );
                Map<String,Object> metrics = annotatedMetrics.metrics().toMap();
                double mean = (double) metrics.get( Metrics.MEAN );
                double error = (double) metrics.get( Metrics.ERROR );
                entries.add( new Entry( benchmark.name(), (error / mean) * 100 ) );
            }
        }
        maxBenchmarkLength = maxBenchmarkLength + 5;
        entries.sort( ( o1, o2 ) -> Double.compare( o2.error(), o1.error() ) );
        DecimalFormat format = new DecimalFormat( "###,##0.00" );
        StringBuilder sb = new StringBuilder()
                .append( "----------------------------------------------------------------------------------------\n" )
                .append( "-------------------------------- STD DEV as % of MEAN ----------------------------------\n" )
                .append( "----------------------------------------------------------------------------------------\n" );
        for ( Entry entry : entries )
        {
            sb.append( format( "\t%1$-" + maxBenchmarkLength + "s %2$10s%%\n",
                               entry.benchmark(), format.format( entry.error() ) ) );
        }
        return sb
                .append( "----------------------------------------------------------------------------------------\n" )
                .toString();
    }

    private static class Entry
    {
        private final String benchmark;
        private double error;

        private Entry( String benchmark, double error )
        {
            this.benchmark = benchmark;
            this.error = error;
        }

        public String benchmark()
        {
            return benchmark;
        }

        public double error()
        {
            return error;
        }
    }

    public static class AnnotatedMetrics
    {
        private final Metrics metrics;
        private final Neo4jConfig neo4jConfig;
        private ProfilerRecordings profilerRecordings;

        public AnnotatedMetrics()
        {
            this( new Metrics(), new Neo4jConfig() );
        }

        AnnotatedMetrics( Metrics metrics, Neo4jConfig neo4jConfig )
        {
            this.metrics = metrics;
            this.neo4jConfig = neo4jConfig;
            this.profilerRecordings = new ProfilerRecordings();
        }

        public Metrics metrics()
        {
            return metrics;
        }

        public Neo4jConfig neo4jConfig()
        {
            return neo4jConfig;
        }

        void attachProfilesRecording( ProfilerRecordings profilerRecordings )
        {
            this.profilerRecordings = profilerRecordings;
        }

        public ProfilerRecordings profilerRecordings()
        {
            return profilerRecordings;
        }

        @Override
        public String toString()
        {
            return metrics.toString() + ((null != profilerRecordings) ? "<Profiled>" : "");
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
            AnnotatedMetrics that = (AnnotatedMetrics) o;
            return Objects.equals( metrics, that.metrics ) &&
                   Objects.equals( neo4jConfig, that.neo4jConfig ) &&
                   Objects.equals( profilerRecordings, that.profilerRecordings );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( metrics, neo4jConfig, profilerRecordings );
        }
    }

    private static class Row
    {
        private final BenchmarkGroup benchmarkGroup;
        private final Benchmark benchmark;
        private final Metrics metrics;
        private final Neo4jConfig neo4jConfig;
        private final ProfilerRecordings profilerRecordings;

        private Row(
                BenchmarkGroup benchmarkGroup,
                Benchmark benchmark,
                Metrics metrics,
                Neo4jConfig neo4jConfig,
                ProfilerRecordings profilerRecordings )
        {
            this.benchmarkGroup = benchmarkGroup;
            this.benchmark = benchmark;
            this.metrics = metrics;
            this.neo4jConfig = neo4jConfig;
            this.profilerRecordings = profilerRecordings;
        }

        //  [
        //      benchmark_group.name:String,    0
        //      benchmark.name:String,          1
        //      benchmark.simple_name:String,   2
        //      benchmark.mode:String,          3
        //      benchmark.description:String,   4
        //      benchmark.parameters:Map,       5
        //      metrics:Map,                    6
        //      neo4j_config:Map                7
        //      profiles:Map                    8  (OPTIONAL)
        //  ]
        private List<Object> toList()
        {
            List<Object> list = new ArrayList<>();
            list.add( benchmarkGroup.name() );
            list.add( benchmark.toMap() );
            list.add( benchmark.parameters() );
            list.add( metrics.toMap() );
            list.add( neo4jConfig.toMap() );
            Map<String,String> profilesMap = profilerRecordings.toMap();
            if ( !profilesMap.isEmpty() )
            {
                list.add( profilesMap );
            }
            return list;
        }

        @Override
        public String toString()
        {
            return format( "%s , %s , %s, %s",
                           benchmarkGroup.name(),
                           benchmark.name(),
                           metrics.toString(),
                           profilerRecordings.toString() );
        }
    }
}
