/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.cli;

import com.github.rvesse.airline.annotations.Command;
import com.google.common.collect.ImmutableList;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.schema.CreateSchema;
import com.neo4j.bench.client.queries.schema.DropSchema;
import com.neo4j.bench.common.command.ResultsStoreArgs;

import java.net.URI;
import java.util.List;
import javax.inject.Inject;

@Command( name = "index" )
public class ReIndexStoreCommand implements Runnable
{

    @Inject
    private final ResultsStoreArgs resultsStoreArgs = new ResultsStoreArgs();

    @Override
    public void run()
    {
        try ( StoreClient client = StoreClient.connect( resultsStoreArgs.resultsStoreUri(),
                                                        resultsStoreArgs.resultsStoreUsername(),
                                                        resultsStoreArgs.resultsStorePassword() ) )
        {
            client.execute( new DropSchema() );
            client.execute( new CreateSchema() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error re-indexing results store: " + resultsStoreArgs.resultsStoreUri(), e );
        }
    }

    public static List<String> argsFor( String resultsStoreUsername,
                                        String resultsStorePassword,
                                        URI resultsStoreUri )
    {
        return ImmutableList.<String>builder()
                .add( "index" )
                .addAll( ResultsStoreArgs.argsFor( resultsStoreUsername, resultsStorePassword, resultsStoreUri ) )
                .build();
    }
}
