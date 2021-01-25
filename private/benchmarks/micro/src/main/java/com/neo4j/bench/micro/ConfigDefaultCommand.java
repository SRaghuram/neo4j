/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Command;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.stream.Collectors.toSet;

@Command( name = "default", description = "creates a benchmark configuration file limited to specified groups" )
public class ConfigDefaultCommand extends ConfigCommandBase
{
    private static final Logger LOG = LoggerFactory.getLogger( ConfigDefaultCommand.class );

    @Override
    public void run()
    {
        LOG.debug( "\n-- Creating Default Config --\n" +
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
