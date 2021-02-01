/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.cli.refactor;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.ImmutableList;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.cli.ResultsStoreCredentials;

import java.net.URI;
import java.util.List;
import javax.inject.Inject;

@Command( name = "verify" )
public class VerifySchemaCommand implements Runnable
{
    private static final int RETRIES = 0;

    @Inject
    @Required
    private ResultsStoreCredentials resultsStoreCredentials;

    @Override
    public void run()
    {
        try ( StoreClient client = StoreClient.connect( resultsStoreCredentials.uri(),
                                                        resultsStoreCredentials.username(),
                                                        resultsStoreCredentials.password(),
                                                        RETRIES ) )
        {
            // verify is called when connecting to store
        }
    }

    public static List<String> argsFor( String resultsStoreUsername,
                                        String resultsStorePassword,
                                        URI resultsStoreUri )
    {
        return ImmutableList.<String>builder()
                .add( "refactor",
                      "verify" )
                .addAll( ResultsStoreCredentials.argsFor( resultsStoreUsername, resultsStorePassword, resultsStoreUri ) )
                .build();
    }
}
