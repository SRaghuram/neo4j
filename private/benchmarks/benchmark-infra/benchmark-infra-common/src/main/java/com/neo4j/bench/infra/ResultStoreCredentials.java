/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.net.URI;

public class ResultStoreCredentials
{

    public static ResultStoreCredentials from( String secretString )
    {
        return JsonUtil.deserializeJson( secretString, ResultStoreCredentials.class );
    }

    private final String username;
    private final String password;
    private final URI uri;

    @JsonCreator
    public ResultStoreCredentials( @JsonProperty( "username" ) String username,
                                   @JsonProperty( "password" ) String password,
                                   @JsonProperty( "uri" ) URI uri )
    {
        this.username = username;
        this.password = password;
        this.uri = uri;
    }

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
}
