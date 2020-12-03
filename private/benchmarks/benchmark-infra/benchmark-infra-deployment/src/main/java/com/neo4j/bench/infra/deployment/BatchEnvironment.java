/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.deployment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.util.Set;

public class BatchEnvironment
{
    private final InstanceType instanceType;
    private final Set<String> jdks;
    private final int vcpus;

    @JsonCreator
    public BatchEnvironment(
            @JsonProperty( "instanceType" )
            @JsonDeserialize( converter = ToInstanceTypeConverter.class )
            @JsonSerialize( converter = FromInstanceTypeConverter.class ) InstanceType instanceType,
            @JsonProperty( "jdks" ) Set<String> jdks,
            @JsonProperty( "vcpus" ) int vcpus )
    {
        this.instanceType = instanceType;
        this.jdks = jdks;
        this.vcpus = vcpus;
    }

    public InstanceType instanceType()
    {
        return instanceType;
    }

    public Set<String> jdks()
    {
        return jdks;
    }

    public int vcpus()
    {
        return vcpus;
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

    public static class ToInstanceTypeConverter extends StdConverter<String,InstanceType>
    {
        @Override
        public InstanceType convert( String value )
        {
            return InstanceType.fromValue( value );
        }
    }

    public static class FromInstanceTypeConverter extends StdConverter<InstanceType,String>
    {

        @Override
        public String convert( InstanceType value )
        {
            return value.toString();
        }
    }
}
