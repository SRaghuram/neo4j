package com.neo4j.bench.micro;

import com.neo4j.bench.micro.config.BenchmarkConfigFile;
import com.neo4j.bench.micro.config.BenchmarkDescription;
import com.neo4j.bench.micro.config.SuiteDescription;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

@Command( name = "benchmarks", description = "creates a benchmark configuration file limited to specified benchmarks" )
public class ConfigBenchmarksCommand extends ConfigCommandBase
{
    @Arguments()
    public List<String> benchmarks = new ArrayList<>();

    @Override
    public void run()
    {
        System.out.println( "\n-- Creating Config --\n" +
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
                .map( BenchmarkDescription::className ).collect(
                        toSet() );

        BenchmarkConfigFile.write(
                suiteDescription,
                enabledBenchmarks,
                verbose,
                withDisabled,
                benchConfigFile.toPath() );
    }
}
