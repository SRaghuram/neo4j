/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class AWSCredentials
{
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final String awsRegion;

    @JsonCreator
    public AWSCredentials( @JsonProperty( "awsAccessKeyId" ) String awsAccessKeyId,
                           @JsonProperty( "awsSecretAccessKey" ) String awsSecretAccessKey,
                           @JsonProperty( "awsRegion" ) String awsRegion )
    {
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretAccessKey = awsSecretAccessKey;
        this.awsRegion = awsRegion;
    }

    public String awsAccessKeyId()
    {
        return awsAccessKeyId;
    }

    public String awsSecretAccessKey()
    {
        return awsSecretAccessKey;
    }

    public String awsRegion()
    {
        return awsRegion;
    }

    public boolean hasAwsCredentials()
    {
        return awsAccessKeyId != null && awsSecretAccessKey != null;
    }

    @Override
    public boolean equals( Object that )
    {
        return EqualsBuilder.reflectionEquals( this, that );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }
}
