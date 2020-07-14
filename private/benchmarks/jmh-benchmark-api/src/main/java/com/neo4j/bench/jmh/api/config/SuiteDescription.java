/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.model.model.BenchmarkConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * Describes final configuration (what will actually be used during execution) of the benchmark suite, including which benchmarks to run and how they are
 * configured.
 */
public class SuiteDescription
{
    public static SuiteDescription fromAnnotations( BenchmarksFinder benchmarksFinder, Validation validation )
    {
        Map<String,BenchmarkDescription> benchmarkDescriptions = benchmarksFinder.getBenchmarks().stream()
                                                                                 .map( clazz -> BenchmarkDescription.of( clazz, validation, benchmarksFinder ) )
                                                                                 .collect( toMap( BenchmarkDescription::className, identity() ) );
        return create( benchmarkDescriptions, validation );
    }

    public static SuiteDescription fromConfig(
            SuiteDescription suiteDescription,
            BenchmarkConfigFile benchmarkConfigFile,
            Validation validation )
    {
        Map<String,BenchmarkDescription> finalBenchmarks = new HashMap<>();
        benchmarkConfigFile.entries()
                           .forEach( configEntry -> addBenchmarkEntryToSuite( configEntry, suiteDescription, finalBenchmarks, validation ) );
        return create( finalBenchmarks, validation );
    }

    public static SuiteDescription fromBenchmarkDescription( BenchmarkDescription benchmarkDescription )
    {
        Map<String,BenchmarkDescription> benchmarks = Collections.singletonMap( benchmarkDescription.className(), benchmarkDescription );
        return new SuiteDescription( benchmarks );
    }

    public static SuiteDescription fromBenchmarkDescriptions( List<BenchmarkDescription> benchmarkDescriptions )
    {
        Map<String,BenchmarkDescription> descriptionMap = benchmarkDescriptions.stream().collect( toMap(
                BenchmarkDescription::className,
                benchmarkDescription -> benchmarkDescription
        ) );

        return new SuiteDescription( descriptionMap );
    }

    private static void addBenchmarkEntryToSuite( BenchmarkConfigFileEntry entry,
                                                  SuiteDescription suite,
                                                  Map<String,BenchmarkDescription> benchmarks,
                                                  Validation validation )
    {
        String configEntryName = entry.name();
        if ( !suite.isBenchmark( configEntryName ) )
        {
            validation.configuredBenchmarkDoesNotExist( configEntryName );
        }
        else
        {
            benchmarks.put( configEntryName,
                            suite.getBenchmark( configEntryName ).copyWithConfig( entry, validation ) );
        }
    }

    private static SuiteDescription create( Map<String,BenchmarkDescription> benchmarks, Validation validation )
    {
        if ( benchmarks.isEmpty() )
        {
            validation.noBenchmarksFound();
        }
        return new SuiteDescription( benchmarks );
    }

    private final Map<String,BenchmarkDescription> benchmarkDescriptions;

    SuiteDescription( Map<String,BenchmarkDescription> benchmarkDescriptions )
    {
        this.benchmarkDescriptions = benchmarkDescriptions;
    }

    public boolean isBenchmark( String benchmarkClassName )
    {
        return benchmarkDescriptions.containsKey( benchmarkClassName );
    }

    public BenchmarkDescription getBenchmark( String benchmarkClassName )
    {
        assertBenchmarkExists( benchmarkClassName );
        return benchmarkDescriptions.get( benchmarkClassName );
    }

    public Set<String> getBenchmarksInGroups( Collection<String> groups )
    {
        groups.forEach( this::assertGroupExists );
        Set<String> groupSet = Sets.newHashSet( groups );
        return benchmarks().stream()
                           .filter( benchmarkDescription -> groupSet.contains( benchmarkDescription.group() ) )
                           .filter( BenchmarkDescription::isEnabled )
                           .map( BenchmarkDescription::className )
                           .collect( toSet() );
    }

    public Collection<BenchmarkDescription> benchmarks()
    {
        return benchmarkDescriptions.values();
    }

    public int count()
    {
        return benchmarkDescriptions.size();
    }

    public BenchmarkConfig toBenchmarkConfig()
    {
        Map<String,String> map = new HashMap<>();
        for ( BenchmarkDescription benchmark : benchmarks() )
        {
            map.put( benchmark.className(), "true" );
            for ( BenchmarkParamDescription param : benchmark.parameters().values() )
            {
                map.put( benchmark.className() + "." + param.name(), String.join( ", ", param.valuesArray() ) );
            }
        }
        return new BenchmarkConfig( map );
    }

    public Map<String,List<String>> getGroupBenchmarkNames()
    {
        Map<String,List<String>> groupBenchmarks = new HashMap<>();
        benchmarks().forEach( benchmarkDescription ->
                                      groupBenchmarks.compute(
                                              benchmarkDescription.group(),
                                              ( k, v ) ->
                                              {
                                                  if ( null == v )
                                                  {
                                                      return Lists.newArrayList( benchmarkDescription.className() );
                                                  }
                                                  else
                                                  {
                                                      v.add( benchmarkDescription.className() );
                                                      return v;
                                                  }
                                              }

                                      ) );
        groupBenchmarks.keySet().forEach( group -> Collections.sort( groupBenchmarks.get( group ) ) );
        return groupBenchmarks;
    }

    /**
     * @implNote It is possible that a benchmark is part of multiple partitions.
     */
    public List<SuiteDescription> partition( int numberOfPartitions )
    {
        List<BenchmarkDescription> enabledExplodedBenchmarks = explodeEnabledBenchmarks();
        int maxPartitions = Math.min( numberOfPartitions, enabledExplodedBenchmarks.size() );

        int benchmarksPerPartition = (int) Math.ceil( enabledExplodedBenchmarks.size() / (double) maxPartitions );

        List<SuiteDescription> partitions = new ArrayList<>( maxPartitions );

        for ( int partition = 0; partition < maxPartitions; partition++ )
        {
            int startIndex = partition * benchmarksPerPartition;
            int endIndex = Math.min( startIndex + benchmarksPerPartition, enabledExplodedBenchmarks.size() );

            List<BenchmarkDescription> rawBenchmarkDescriptionPartition = enabledExplodedBenchmarks.subList( startIndex, endIndex );

            List<BenchmarkDescription> condensedBenchmarkDescriptionPartition =
                    BenchmarkDescription.implode( new HashSet<>( rawBenchmarkDescriptionPartition ) );

            partitions.add( SuiteDescription.fromBenchmarkDescriptions( condensedBenchmarkDescriptionPartition ) );
        }

        return partitions;
    }

    public List<BenchmarkDescription> explodeEnabledBenchmarks()
    {
        return benchmarks()
                .stream()
                .filter( BenchmarkDescription::isEnabled )
                .flatMap( benchmark -> benchmark.explode().stream() )
                .collect( toList() );
    }

    private void assertBenchmarkExists( String maybeBenchmark )
    {
        if ( !isBenchmark( maybeBenchmark ) )
        {
            throw new RuntimeException( "Unrecognized benchmark: " + maybeBenchmark );
        }
    }

    private void assertGroupExists( String maybeGroup )
    {
        if ( !isGroup( maybeGroup ) )
        {
            throw new RuntimeException( "Unrecognized benchmark group: " + maybeGroup );
        }
    }

    private boolean isGroup( String maybeGroup )
    {
        return benchmarkDescriptions.values().stream()
                                    .map( BenchmarkDescription::group )
                                    .anyMatch( group -> group.equals( maybeGroup ) );
    }
}
