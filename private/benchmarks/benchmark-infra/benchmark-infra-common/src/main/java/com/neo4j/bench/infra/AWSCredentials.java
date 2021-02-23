/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringExclude;
import org.apache.commons.lang3.builder.ToStringStyle;

public class AWSCredentials
{
    private final String awsAccessKeyId;
    @ToStringExclude
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

    public AWSCredentialsProvider awsCredentialsProvider()
    {
        if ( hasAwsCredentials() )
        {
            return new AWSStaticCredentialsProvider( new BasicAWSCredentials( awsAccessKeyId(), awsSecretAccessKey() ) );
        }
        else
        {
            return DefaultAWSCredentialsProviderChain.getInstance();
        }
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

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }
}
