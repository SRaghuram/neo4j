/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.client.queries.DropSchema;

import java.net.URI;

@Command( name = "index" )
public class ReIndexStoreCommand implements Runnable
{
    public static final String CMD_RESULTS_STORE_USER = "--results_store_user";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_USER},
            description = "Username for Neo4j database server that stores benchmarking results",
            title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;

    public static final String CMD_RESULTS_STORE_PASSWORD = "--results_store_pass";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_PASSWORD},
            description = "Password for Neo4j database server that stores benchmarking results",
            title = "Results Store Password" )
    @Required
    private String resultsStorePassword;

    public static final String CMD_RESULTS_STORE_URI = "--results_store_uri";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_URI},
            description = "URI to Neo4j database server for storing benchmarking results",
            title = "Results Store" )
    @Required
    private URI resultsStoreUri;

    @Override
    public void run()
    {
        try ( StoreClient client = StoreClient.connect( resultsStoreUri, resultsStoreUsername, resultsStorePassword ) )
        {
            client.execute( new DropSchema() );
            client.execute( new CreateSchema() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error re-indexing results store: " + resultsStoreUri, e );
        }
    }
}
