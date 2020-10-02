/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.BenchmarkDescription;
import com.neo4j.bench.jmh.api.config.BenchmarksFinder;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;

import java.io.File;
import java.util.List;
import java.util.Set;

import static com.neo4j.bench.jmh.api.config.Validation.assertValid;
import static java.util.stream.Collectors.toSet;

@Command( name = "partition", description = "creates a benchmark configuration file limited to specified groups" )
public class PartitionConfigCommand implements Runnable
{
    @Required
    @Option( type = OptionType.COMMAND,
            name = {"-p", "--number-of-partitions"},
            description = "The number of partitions that should be generated",
            title = "partitions" )
    protected int partitions;

    @Required
    @Option( type = OptionType.COMMAND,
            name = {"-d", "--directory"},
            description = "The directory that the config files will be generated",
            title = "Directory" )
    protected File configDirectory;

    @Required
    @Option( type = OptionType.COMMAND,
            name = {"--config-path"},
            description = "Path to config file that will be partition",
            title = "Configuration Path" )
    protected File benchConfigFile;

    public void run()
    {
        System.out.printf( "%n-- Partitioning Config --%n" +
                           "\tBase Config:      %s%n" +
                           "\tDirectory:        %s%n" +
                           "\tPartitions:       %s%n%n",
                           benchConfigFile.toPath().toAbsolutePath(),
                           configDirectory.toPath().toAbsolutePath(),
                           partitions );

        if ( partitions < 2 )
        {
            throw new RuntimeException( String.format( "Expected at least two partitions, got %s specified", partitions ) );
        }

        Validation validation = new Validation();
        BenchmarksFinder benchmarksFinder = new BenchmarksFinder( BenchmarksRunner.class.getPackage().getName() );

        SuiteDescription suiteDescriptionFromFile = SuiteDescription.fromConfig( ConfigCommandBase.allBenchmarks(),
                                                                                 BenchmarkConfigFile.fromFile( benchConfigFile.toPath(),
                                                                                                               validation,
                                                                                                               benchmarksFinder ),
                                                                                 validation );
        assertValid(validation);
        List<SuiteDescription> partitions = suiteDescriptionFromFile.partition( this.partitions );

        for ( int i = 0; i < partitions.size(); i++ )
        {
            SuiteDescription suitePartition = partitions.get( i );
            File partitionConfigFile = new File( configDirectory, String.format( "micro_%s.conf", i ) );
            Set<String> enabledBenchmarks = suitePartition.benchmarks().stream()
                                                         .map( BenchmarkDescription::className )
                                                         .collect( toSet() );
            BenchmarkConfigFile.write(
                    suitePartition,
                    enabledBenchmarks,
                    false,
                    false,
                    partitionConfigFile.toPath() );
        }
    }
}
