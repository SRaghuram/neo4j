package com.neo4j.bench.micro;

import com.neo4j.bench.micro.config.BenchmarkConfigFile;
import com.neo4j.bench.micro.config.SuiteDescription;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

@Command( name = "groups", description = "creates a benchmark configuration file limited to specified groups" )
public class ConfigGroupCommand extends ConfigCommandBase
{
    @Arguments()
    public List<String> groups = new ArrayList<>();

    @Override
    public void run()
    {
        System.out.println( format( "\n-- Creating Config --\n" +
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
