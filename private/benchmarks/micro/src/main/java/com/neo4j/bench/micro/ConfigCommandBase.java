/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.jmh.api.config.BenchmarksFinder;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;

import java.io.File;

import static java.lang.String.format;

abstract class ConfigCommandBase implements Runnable
{
    @Required
    @Option( type = OptionType.COMMAND,
            name = {"--path"},
            description = "Path to export the generated benchmark configuration file to",
            title = "Configuration Path" )
    protected File benchConfigFile;

    @Option( type = OptionType.COMMAND,
            name = {"-v", "--verbose"},
            description = "Write benchmark parameters to config file, in addition to enable/disable info",
            title = "Verbose" )
    protected boolean verbose;

    @Option( type = OptionType.COMMAND,
            name = {"-w", "--with-disabled"},
            description = "Write disabled benchmarks, in addition to the enabled",
            title = "Verbose" )
    protected boolean withDisabled;

    protected String baseOptionString()
    {
        return format( "\tPath:           %s\n" +
                       "\tVerbose:        %s\n" +
                       "\tWith disabled:  %s",
                       benchConfigFile.toPath().toAbsolutePath(),
                       verbose,
                       withDisabled );
    }

    public static SuiteDescription allBenchmarks()
    {
        Validation validation = new Validation();
        BenchmarksFinder benchmarksFinder = new BenchmarksFinder( BenchmarksRunner.class.getPackage().getName() );
        SuiteDescription allByReflection = SuiteDescription.fromAnnotations( benchmarksFinder, validation );
        Validation.assertValid( validation );
        return allByReflection;
    }
}
