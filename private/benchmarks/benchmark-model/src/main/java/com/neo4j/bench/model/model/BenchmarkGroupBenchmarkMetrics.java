/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;
import com.neo4j.bench.model.profiling.ProfilerRecordings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Collection;
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
    public static class BenchmarkMetrics
    {
        @JsonSerialize( keyUsing = Benchmark.BenchmarkKeySerializer.class )
        @JsonDeserialize( keyUsing = Benchmark.BenchmarkKeyDeserializer.class )
        private final Map<Benchmark,AnnotatedMetrics> inner = new HashMap<>();

        private Collection<Benchmark> benchmarks()
        {
            return inner.keySet();
        }

        private AnnotatedMetrics getMetricsFor( Benchmark benchmark )
        {
            return inner.get( benchmark );
        }

        public boolean containsBenchmark( Benchmark benchmark )
        {
            return inner.containsKey( benchmark );
        }

        public void put( Benchmark benchmark, AnnotatedMetrics annotatedMetrics )
        {
            inner.put( benchmark, annotatedMetrics );
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

    @JsonSerialize( keyUsing = BenchmarkGroup.BenchmarkGroupKeySerializer.class )
    @JsonDeserialize( keyUsing = BenchmarkGroup.BenchmarkGroupKeyDeserializer.class )
    private final Map<BenchmarkGroup,BenchmarkMetrics> inner = new HashMap<>();

    public void addAll( BenchmarkGroupBenchmarkMetrics other )
    {
        requireNonNull( other );
        for ( BenchmarkGroup group : other.inner.keySet() )
        {
            BenchmarkMetrics annotatedBenchmarks = other.inner.get( group );
            for ( Benchmark benchmark : annotatedBenchmarks.benchmarks() )
            {
                AnnotatedMetrics annotatedMetrics = annotatedBenchmarks.getMetricsFor( benchmark );
                add( group, benchmark, annotatedMetrics.metrics(), annotatedMetrics.maybeAuxiliaryMetrics(), annotatedMetrics.neo4jConfig() );
            }
        }
    }

    public void add( BenchmarkGroup group, Benchmark benchmark, Metrics metrics, Metrics maybeAuxiliaryMetrics, Neo4jConfig neo4jConfig )
    {
        requireNonNull( group );
        requireNonNull( benchmark );
        requireNonNull( metrics );
        // auxiliary metrics may be null
        requireNonNull( neo4jConfig );
        assertSaneModeAndUnit( benchmark, metrics );

        BenchmarkMetrics benchmarkMetrics = inner.computeIfAbsent( group, key -> new BenchmarkMetrics() );
        if ( benchmarkMetrics.containsBenchmark( benchmark ) )
        {
            throw new IllegalStateException( format( "Multiple results for benchmark: %s\nExisting: %s\nNew: %s",
                                                     benchmark,
                                                     benchmarkMetrics.getMetricsFor( benchmark ).metrics(),
                                                     metrics ) );
        }
        benchmarkMetrics.put( benchmark, new AnnotatedMetrics( metrics, maybeAuxiliaryMetrics, neo4jConfig ) );
    }

    private static void assertSaneModeAndUnit( Benchmark benchmark, Metrics metrics )
    {
        if ( !metrics.unit().isCompatibleMode( benchmark.mode() ) )
        {
            throw new IllegalStateException( format( "Metrics unit '%s' is not compatible with Benchmark mode '%s'", metrics.unit(), benchmark.mode() ) );
        }
    }

    public List<BenchmarkGroupBenchmark> benchmarkGroupBenchmarks()
    {
        return inner.keySet().stream()
                    .flatMap( group -> inner.get( group ).benchmarks().stream()
                                            .map( benchmark -> new BenchmarkGroupBenchmark( group, benchmark ) ) )
                    .collect( Collectors.toList() );
    }

    public void attachProfilerRecording( BenchmarkGroup group, Benchmark benchmark, ProfilerRecordings profilerRecordings )
    {
        inner.get( group ).getMetricsFor( benchmark ).attachProfilesRecording( profilerRecordings );
    }

    public AnnotatedMetrics getMetricsFor( BenchmarkGroupBenchmark benchmarkGroupBenchmark )
    {
        return getMetricsFor( benchmarkGroupBenchmark.benchmarkGroup(), benchmarkGroupBenchmark.benchmark() );
    }

    public AnnotatedMetrics getMetricsFor( BenchmarkGroup group, Benchmark benchmark )
    {
        return inner.get( group ).getMetricsFor( benchmark );
    }

    public List<List<Object>> toList()
    {
        return toRows().stream().map( Row::toList ).collect( Collectors.toList() );
    }

    private List<Row> toRows()
    {
        List<Row> list = new ArrayList<>();
        for ( Map.Entry<BenchmarkGroup,BenchmarkMetrics> groupEntry : inner.entrySet() )
        {
            BenchmarkGroup group = groupEntry.getKey();
            BenchmarkMetrics benchmarkMetrics = groupEntry.getValue();
            for ( Benchmark benchmark : benchmarkMetrics.benchmarks() )
            {
                AnnotatedMetrics annotatedMetrics = benchmarkMetrics.getMetricsFor( benchmark );
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

    public static class AnnotatedMetrics
    {
        private final Metrics metrics;
        private final Metrics auxiliaryMetrics;
        private final Neo4jConfig neo4jConfig;
        private ProfilerRecordings profilerRecordings;

        public AnnotatedMetrics()
        {
            this( new Metrics(), new Metrics(), Neo4jConfig.empty() );
        }

        public AnnotatedMetrics( Metrics metrics, Metrics auxiliaryMetrics, Neo4jConfig neo4jConfig )
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
         * @return {@link Metrics} instance, or null if none exists
         */
        public Metrics maybeAuxiliaryMetrics()
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
        private final Metrics auxiliaryMetrics;
        private final BenchmarkGroup benchmarkGroup;
        private final Neo4jConfig neo4jConfig;
        private final ProfilerRecordings profilerRecordings;

        private Row(
                BenchmarkGroup benchmarkGroup,
                Benchmark benchmark,
                Metrics metrics,
                Metrics auxiliaryMetrics,
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
        //      benchmark:Map,                  1
        //        * name:String
        //        * simple_name:String
        //        * mode:String
        //        * cypher_query:String
        //        * description:String
        //      benchmark.parameters:Map,       2
        //      metrics:Map,                    3
        //      neo4j_config:Map                4
        //      profiles:List[Map]              5
        //      auxiliary_metrics:List[Map]     6
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
                      : Lists.newArrayList( auxiliaryMetrics.toMap() ) );
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
