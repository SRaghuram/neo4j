/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

@Command( name = "groups", description = "creates a benchmark configuration file limited to specified groups" )
public class ConfigGroupCommand extends ConfigCommandBase
{
    private static final Logger LOG = LoggerFactory.getLogger( ConfigGroupCommand.class );

    @Arguments()
    public List<String> groups = new ArrayList<>();

    @Override
    public void run()
    {
        LOG.debug( format( "\n-- Creating Config --\n" +
                                    "\tGroups:         %s\n" +
                                    "%s", groups, baseOptionString() ) );

        if ( groups.isEmpty() )
        {
            throw new RuntimeException( "Expected at least one group, none specified" );
        }

        SuiteDescription suiteDescription = allBenchmarks();

        BenchmarkConfigFile.write(
                suiteDescription,
                suiteDescription.getBenchmarksInGroups( groups ),
                verbose,
                withDisabled,
                benchConfigFile.toPath() );
    }
}
