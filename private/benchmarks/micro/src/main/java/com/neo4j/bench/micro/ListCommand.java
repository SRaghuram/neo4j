/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.neo4j.bench.micro.config.SuiteDescription;
import com.neo4j.bench.micro.config.Validation;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.util.List;
import java.util.Map;

@Command( name = "ls", description = "prints available groups and their benchmarks" )
public class ListCommand implements Runnable
{
    @Option( type = OptionType.COMMAND,
            name = {"-v", "--verbose"},
            description = "Print benchmark names, in addition to group names",
            title = "Verbose",
            required = false )
    protected boolean verbose;

    @Override
    public void run()
    {
        Validation validation = new Validation();
        SuiteDescription suite = SuiteDescription.byReflection( validation );
        Validation.assertValid( validation );
        Map<String,List<String>> groupBenchmarks = suite.getGroupBenchmarkNames();
        System.out.println( makeGroupBenchmarksString( groupBenchmarks, verbose ) );
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
