/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.cli;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;

import java.net.URI;
import java.util.List;

public class ResultsStoreCredentials
{
    private static final String CMD_RESULTS_STORE_USER = "--results-store-user";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_USER},
            description = "Username for Neo4j database server that stores benchmarking results",
            title = "Results Store Username" )
    @Required
    private String username;

    private static final String CMD_RESULTS_STORE_PASSWORD = "--results-store-pass";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_PASSWORD},
            description = "Password for Neo4j database server that stores benchmarking results",
            title = "Results Store Password" )
    @Required
    private String password;

    private static final String CMD_RESULTS_STORE_URI = "--results-store-uri";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_URI},
            description = "URI to Neo4j database server for storing benchmarking results",
            title = "Results Store" )
    @Required
    private URI uri;

    public String username()
    {
        return username;
    }

    public String password()
    {
        return password;
    }

    public URI uri()
    {
        return uri;
    }

    public static List<String> argsFor( String username,
                                        String password,
                                        URI resultsStoreUri )
    {
        return argsFor( username, password, resultsStoreUri.toString() );
    }

    public static List<String> argsFor( String username,
                                        String password,
                                        String uri )
    {
        return Lists.newArrayList( ResultsStoreCredentials.CMD_RESULTS_STORE_USER,
                                   username,
                                   ResultsStoreCredentials.CMD_RESULTS_STORE_PASSWORD,
                                   password,
                                   ResultsStoreCredentials.CMD_RESULTS_STORE_URI,
                                   uri );
    }
}
