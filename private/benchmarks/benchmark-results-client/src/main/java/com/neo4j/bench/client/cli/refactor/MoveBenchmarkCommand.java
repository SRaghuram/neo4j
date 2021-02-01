/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.cli.refactor;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.ImmutableList;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.cli.ResultsStoreCredentials;
import com.neo4j.bench.client.queries.refactor.MoveBenchmarkQuery;
import com.neo4j.bench.model.model.BenchmarkGroup;

import java.net.URI;
import java.util.List;
import javax.inject.Inject;

@Command( name = "move-benchmark" )
public class MoveBenchmarkCommand implements Runnable
{
    @Inject
    @Required
    private ResultsStoreCredentials resultsStoreCredentials;

    private static final String CMD_BENCHMARK_TOOL_NAME = "--benchmark-tool-name";
    @Option( type = OptionType.COMMAND,
            name = {CMD_BENCHMARK_TOOL_NAME},
            description = "Repository name of tool used to generate the benchmark, i.e. 'micro' or 'ldbc'. " +
                          "See com.neo4j.bench.model.model.Repository for details.",
            title = "Benchmark Tool Name" )
    @Required
    private String benchmarkToolName;

    private static final String CMD_OLD_BENCHMARK_GROUP_NAME = "--old-benchmark-group-name";
    @Option( type = OptionType.COMMAND,
            name = {CMD_OLD_BENCHMARK_GROUP_NAME},
            description = "Name of the BenchmarkGroup from which Benchmarks will be moved, i.e. 'algos'",
            title = "Old BenchmarkGroup Name" )
    @Required
    private String oldGroupName;

    private static final String CMD_NEW_BENCHMARK_GROUP_NAME = "--new-benchmark-group-name";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEW_BENCHMARK_GROUP_NAME},
            description = "Name of the BenchmarkGroup to which Benchmark will be moved, i.e. 'core'",
            title = "New BenchmarkGroup Name" )
    @Required
    private String newGroupName;

    private static final String CMD_BENCHMARK_NAME = "--benchmark-name";
    @Option( type = OptionType.COMMAND,
            name = {CMD_BENCHMARK_NAME},
            description = "Name of the Benchmark that will be moved, i.e. 'ConcurrentReadWriteLabels.createDeleteLabel` or `LdbcQuery10`",
            title = "Benchmark Name" )
    @Required
    private String benchmarkName;

    @Override
    public void run()
    {
        try ( StoreClient client = StoreClient.connect( resultsStoreCredentials.uri(),
                                                        resultsStoreCredentials.username(),
                                                        resultsStoreCredentials.password() ) )
        {
            BenchmarkGroup oldGroup = new BenchmarkGroup( oldGroupName );
            BenchmarkGroup newGroup = new BenchmarkGroup( newGroupName );
            client.execute( new MoveBenchmarkQuery( benchmarkToolName, oldGroup, newGroup, benchmarkName ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error moving group: " + resultsStoreCredentials.uri(), e );
        }
    }

    public static List<String> argsFor( String resultsStoreUsername,
                                        String resultsStorePassword,
                                        URI resultsStoreUri,
                                        String benchmarkToolName,
                                        String oldGroupName,
                                        String newGroupName,
                                        String benchmarkName )
    {
        return ImmutableList.<String>builder()
                .add( "refactor",
                      "move-benchmark",
                      CMD_BENCHMARK_TOOL_NAME,
                      benchmarkToolName,
                      CMD_OLD_BENCHMARK_GROUP_NAME,
                      oldGroupName,
                      CMD_NEW_BENCHMARK_GROUP_NAME,
                      newGroupName,
                      CMD_BENCHMARK_NAME,
                      benchmarkName )
                .addAll( ResultsStoreCredentials.argsFor( resultsStoreUsername, resultsStorePassword, resultsStoreUri ) )
                .build();
    }
}
