/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Command;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.SuiteDescription;

import java.util.Set;

import static java.util.stream.Collectors.toSet;

@Command( name = "default", description = "creates a benchmark configuration file limited to specified groups" )
public class ConfigDefaultCommand extends ConfigCommandBase
{
    @Override
    public void run()
    {
        System.out.println( "\n-- Creating Default Config --\n" +
                            baseOptionString() );

        SuiteDescription suiteDescription = allBenchmarks();
        Set<String> enabledBenchmarks = suiteDescription.benchmarks().stream()
                                                        .filter( BenchmarkDescription::isEnabled )
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
