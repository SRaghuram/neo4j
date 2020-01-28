/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.model;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

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
            for ( var entry : annotatedBenchmarks.entrySet() )
            {
                Benchmark benchmark = entry.getKey();
                AnnotatedMetrics annotatedMetrics = entry.getValue();
                add( group, benchmark, annotatedMetrics.metrics(), annotatedMetrics.maybeAuxiliaryMetrics(), annotatedMetrics.neo4jConfig() );
            }
        }
    }

    public void add( BenchmarkGroup group, Benchmark benchmark, Metrics metrics, AuxiliaryMetrics maybeAuxiliaryMetrics, Neo4jConfig neo4jConfig )
    {
        requireNonNull( group );
        requireNonNull( benchmark );
        requireNonNull( metrics );
        // auxiliary metrics may be null
        requireNonNull( neo4jConfig );

        Map<Benchmark,AnnotatedMetrics> benchmarkMetrics = inner.computeIfAbsent( group, key -> new HashMap<>() );
        if ( benchmarkMetrics.containsKey( benchmark ) )
        {
            throw new IllegalStateException( format( "Multiple results for benchmark: %s\nExisting: %s\nNew: %s",
                                                     benchmark,
                                                     benchmarkMetrics.get( benchmark ).metrics(),
                                                     metrics ) );
        }
        benchmarkMetrics.put( benchmark, new AnnotatedMetrics( metrics, maybeAuxiliaryMetrics, neo4jConfig ) );
    }

    public List<BenchmarkGroupBenchmark> benchmarkGroupBenchmarks()
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
                        annotatedMetrics.maybeAuxiliaryMetrics(),
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
        private final AuxiliaryMetrics auxiliaryMetrics;
        private final Neo4jConfig neo4jConfig;
        private ProfilerRecordings profilerRecordings;

        public AnnotatedMetrics()
        {
            this( new Metrics(), new AuxiliaryMetrics(), Neo4jConfig.empty() );
        }

        AnnotatedMetrics( Metrics metrics, AuxiliaryMetrics auxiliaryMetrics, Neo4jConfig neo4jConfig )
        {
            this.metrics = metrics;
            this.auxiliaryMetrics = auxiliaryMetrics;
            this.neo4jConfig = neo4jConfig;
            this.profilerRecordings = new ProfilerRecordings();
        }

        public Metrics metrics()
        {
            return metrics;
        }

        /**
         * @return {@link AuxiliaryMetrics} instance, or null if none exists
         */
        public AuxiliaryMetrics maybeAuxiliaryMetrics()
        {
            return auxiliaryMetrics;
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
            return EqualsBuilder.reflectionEquals( this, o );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }

    private static class Row
    {
        private final Benchmark benchmark;
        private final Metrics metrics;
        private final AuxiliaryMetrics auxiliaryMetrics;
        private final BenchmarkGroup benchmarkGroup;
        private final Neo4jConfig neo4jConfig;
        private final ProfilerRecordings profilerRecordings;

        private Row(
                BenchmarkGroup benchmarkGroup,
                Benchmark benchmark,
                Metrics metrics,
                AuxiliaryMetrics auxiliaryMetrics,
                Neo4jConfig neo4jConfig,
                ProfilerRecordings profilerRecordings )
        {
            this.benchmarkGroup = benchmarkGroup;
            this.benchmark = benchmark;
            this.metrics = metrics;
            this.auxiliaryMetrics = auxiliaryMetrics;
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
        //      profiles:List[Map]              8
        //      auxiliary_metrics:List[Map]     9
        //  ]
        private List<Object> toList()
        {
            List<Object> list = new ArrayList<>();
            list.add( benchmarkGroup.name() );
            list.add( benchmark.toMap() );
            list.add( benchmark.parameters() );
            list.add( metrics.toMap() );
            list.add( neo4jConfig.toMap() );
            // for 'profiles' we want to always add an element to the row, if there is no profiler recording information we add an empty list
            Map<String,String> profilerRecordingsMap = profilerRecordings.toMap();
            list.add( profilerRecordingsMap.isEmpty()
                      ? new ArrayList<>()
                      : Lists.newArrayList( profilerRecordingsMap ) );
            // for 'auxiliary_metrics' we want to always add an element to the row, if there are no auxiliary metrics we add an empty list
            list.add( null == auxiliaryMetrics
                      ? new ArrayList<>()
                      : Lists.newArrayList( auxiliaryMetrics ) );
            return list;
        }

        @Override
        public String toString()
        {
            return format( "%s , %s , %s, %s, %s",
                           benchmarkGroup.name(),
                           benchmark.name(),
                           metrics,
                           auxiliaryMetrics,
                           profilerRecordings );
        }
    }
}
