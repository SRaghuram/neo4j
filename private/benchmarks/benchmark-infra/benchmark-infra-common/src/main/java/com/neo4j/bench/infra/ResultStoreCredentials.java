/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.net.URI;

public class ResultStoreCredentials
{

    public static ResultStoreCredentials from( String secretString )
    {
        return JsonUtil.deserializeJson( secretString, ResultStoreCredentials.class );
    }

    private final String username;
    @ToStringExclude
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

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }
}
