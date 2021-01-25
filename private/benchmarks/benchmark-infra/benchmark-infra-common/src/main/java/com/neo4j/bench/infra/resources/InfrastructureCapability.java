/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Describes infrastructure resource capability, like amount of total memory, available CPUs and so on.
 *
 * @param <T>
 */
public abstract class InfrastructureCapability<T>
{

    private final T value;

    protected InfrastructureCapability( T value )
    {
        this.value = Objects.requireNonNull( value );
    }

    public T value()
    {
        return value;
    }

    /**
     * @return predicate which matches infrastructure capability
     */
    public abstract Predicate<InfrastructureCapability<?>> predicate();

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( value ).build();
    }
}
