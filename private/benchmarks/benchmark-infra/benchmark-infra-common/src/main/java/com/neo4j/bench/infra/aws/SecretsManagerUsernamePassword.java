/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.aws;

import com.neo4j.bench.model.util.JsonUtil;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Objects;

public class SecretsManagerUsernamePassword
{

    public static SecretsManagerUsernamePassword from( String secretString )
    {
        return JsonUtil.deserializeJson( secretString, SecretsManagerUsernamePassword.class );
    }

    private String username;
    private String password;

    // used by JSON serialization only
    private SecretsManagerUsernamePassword()
    {

    }

    public SecretsManagerUsernamePassword( String username, String password )
    {
        Objects.requireNonNull( username );
        Objects.requireNonNull( password );
        this.username = username;
        this.password = password;
    }

    public String username()
    {
        return username;
    }

    public String password()
    {
        return password;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        SecretsManagerUsernamePassword that = (SecretsManagerUsernamePassword) o;
        return EqualsBuilder.reflectionEquals( this, that );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }
}
