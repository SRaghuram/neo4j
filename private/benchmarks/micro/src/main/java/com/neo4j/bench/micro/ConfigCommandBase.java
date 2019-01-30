/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.neo4j.bench.micro.config.SuiteDescription;
import com.neo4j.bench.micro.config.Validation;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;

import static java.lang.String.format;

abstract class ConfigCommandBase implements Runnable
{
    @Option( type = OptionType.COMMAND,
            name = {"--path"},
            description = "Path to export the generated benchmark configuration file to",
            title = "Configuration Path",
            required = true )
    protected File benchConfigFile;

    @Option( type = OptionType.COMMAND,
            name = {"-v", "--verbose"},
            description = "Write benchmark parameters to config file, in addition to enable/disable info",
            title = "Verbose",
            required = false )
    protected boolean verbose;

    @Option( type = OptionType.COMMAND,
            name = {"-w", "--with-disabled"},
            description = "Write disabled benchmarks, in addition to the enabled",
            title = "Verbose",
            required = false )
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

    protected SuiteDescription allBenchmarks()
    {
        Validation validation = new Validation();
        SuiteDescription allByReflection = SuiteDescription.byReflection( validation );
        Validation.assertValid( validation );
        return allByReflection;
    }
}
