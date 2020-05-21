/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

@Command( name = "benchmarks", description = "creates a benchmark configuration file limited to specified benchmarks" )
public class ConfigBenchmarksCommand extends ConfigCommandBase
{
    private static final Logger LOG = LoggerFactory.getLogger( ConfigBenchmarksCommand.class );

    @Arguments()
    public List<String> benchmarks = new ArrayList<>();

    @Override
    public void run()
    {
        LOG.debug( "\n-- Creating Config --\n" +
                            format( "\tBenchmarks:     %s\n", benchmarks ) +
                            baseOptionString() );

        if ( benchmarks.isEmpty() )
        {
            throw new RuntimeException( "Expected at least one benchmark, none specified" );
        }

        SuiteDescription suiteDescription = allBenchmarks();

        // this forces existence of selected benchmarks to be checked
        Set<String> enabledBenchmarks = benchmarks.stream()
                                                  .map( suiteDescription::getBenchmark )
                                                  .map( BenchmarkDescription::className )
                                                  .collect( toSet() );

        BenchmarkConfigFile.write(
                suiteDescription,
                enabledBenchmarks,
                verbose,
                withDisabled,
                benchConfigFile.toPath() );
    }
}
