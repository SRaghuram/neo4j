/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.command;

import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.net.URI;
import java.util.List;

public class ResultsStoreArgs
{

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
        return Lists.newArrayList( CMD_RESULTS_STORE_USER,
                                   username,
                                   CMD_RESULTS_STORE_PASSWORD,
                                   password,
                                   CMD_RESULTS_STORE_URI,
                                   uri );
    }

    public static final String CMD_RESULTS_STORE_USER = "--results-store-user";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_USER},
            description = "Username for Neo4j database server that stores benchmarking results",
            title = "Results Store Username" )
    private String resultsStoreUsername;

    public static final String CMD_RESULTS_STORE_PASSWORD = "--results-store-pass";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_PASSWORD},
            description = "Password for Neo4j database server that stores benchmarking results",
            title = "Results Store Password" )
    private String resultsStorePassword;

    public static final String CMD_RESULTS_STORE_URI = "--results-store-uri";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_STORE_URI},
            description = "URI to Neo4j database server for storing benchmarking results",
            title = "Results Store" )
    private URI resultsStoreUri;

    public String resultsStoreUsername()
    {
        return resultsStoreUsername;
    }

    public String resultsStorePassword()
    {
        return resultsStorePassword;
    }

    public URI resultsStoreUri()
    {
        return resultsStoreUri;
    }

    @Override
    public boolean equals( Object o )
    {

        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }
}
