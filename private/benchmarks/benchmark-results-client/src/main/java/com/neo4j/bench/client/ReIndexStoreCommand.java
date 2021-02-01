/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.ImmutableList;
import com.neo4j.bench.client.cli.ResultsStoreCredentials;
import com.neo4j.bench.client.queries.schema.CreateSchema;
import com.neo4j.bench.client.queries.schema.DropSchema;

import java.net.URI;
import java.util.List;
import javax.inject.Inject;

@Command( name = "index" )
public class ReIndexStoreCommand implements Runnable
{
    @Inject
    @Required
    private ResultsStoreCredentials resultsStoreCredentials;

    @Override
    public void run()
    {
        try ( StoreClient client = StoreClient.connect( resultsStoreCredentials.uri(),
                                                        resultsStoreCredentials.username(),
                                                        resultsStoreCredentials.password() ) )
        {
            client.execute( new DropSchema() );
            client.execute( new CreateSchema() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error re-indexing results store: " + resultsStoreCredentials.uri(), e );
        }
    }

    public static List<String> argsFor( String resultsStoreUsername,
                                        String resultsStorePassword,
                                        URI resultsStoreUri )
    {
        return ImmutableList.<String>builder()
                .add( "index" )
                .addAll( ResultsStoreCredentials.argsFor( resultsStoreUsername, resultsStorePassword, resultsStoreUri ) )
                .build();
    }
}
