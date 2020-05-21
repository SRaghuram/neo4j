/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.neo4j.bench.jmh.api.config.BenchmarksFinder;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

@Command( name = "ls", description = "prints available groups and their benchmarks" )
public class ListCommand implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger( ListCommand.class );

    @Option( type = OptionType.COMMAND,
            name = {"-v", "--verbose"},
            description = "Print benchmark names, in addition to group names",
            title = "Verbose" )
    protected boolean verbose;

    @Override
    public void run()
    {
        Validation validation = new Validation();
        BenchmarksFinder benchmarksFinder = new BenchmarksFinder( "com.neo4j.bench.micro" );
        SuiteDescription suite = SuiteDescription.fromAnnotations( benchmarksFinder, validation );
        Validation.assertValid( validation );
        Map<String,List<String>> groupBenchmarks = suite.getGroupBenchmarkNames();
        LOG.debug( makeGroupBenchmarksString( groupBenchmarks, verbose ) );
    }

    private static String makeGroupBenchmarksString( Map<String,List<String>> groupBenchmarks, boolean verbose )
    {
        String title = verbose ? "Available Groups & Benchmarks\n" : "Available Groups\n";
        StringBuilder sb = new StringBuilder( title );
        groupBenchmarks.keySet().forEach( group ->
                                          {
                                              sb.append( "\t" ).append( group ).append( "\n" );
                                              if ( verbose )
                                              {
                                                  groupBenchmarks.get( group ).forEach( benchmark ->
                                                                                                sb.append( "\t\t" ).append( benchmark ).append( "\n" ) );
                                              }
                                          } );
        return sb.toString();
    }
}
