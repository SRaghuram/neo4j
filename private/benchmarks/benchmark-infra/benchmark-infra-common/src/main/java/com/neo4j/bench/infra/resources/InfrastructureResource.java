/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Set;

import static java.lang.String.format;

/**
 * Base for AWS infrastructure resources, like job queues or compute environments.
 */
public abstract class InfrastructureResource
{
    private final Set<InfrastructureCapability> capabilities;

    /**
     * Constructs infrastructure resource ( e.g. job queue or compute environment) with provided set of capabilities.
     */
    protected InfrastructureResource( InfrastructureCapability... capabilities )
    {
        this.capabilities = ImmutableSet.copyOf( capabilities );
    }

    Set<InfrastructureCapability> capabilities()
    {
        return capabilities;
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

    /**
     * Lookup for capability.
     *
     * @param capabilityType expected capability type
     * @param <T>
     * @return found capability
     * @throws IllegalArgumentException if this resource doesn't have e
     */
    public <T extends InfrastructureCapability<?>> T getCapabilityFor( Class<T> capabilityType )
    {
        return capabilities().stream()
                             .filter( capabilityType::isInstance )
                             .map( capabilityType::cast )
                             .findFirst()
                             .orElseThrow(
                                     () -> new IllegalArgumentException( format( "'%s' does not support capability '%s'", this, capabilityType ) ) );
    }
}
