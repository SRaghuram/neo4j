/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.monitoring;

import java.time.Instant;
import java.util.Objects;

class Sample<T>
{
    private final Instant instant;
    private final T value;

    Sample( Instant instant, T value )
    {
        this.instant = instant;
        this.value = value;
    }

    Instant instant()
    {
        return instant;
    }

    T value()
    {
        return value;
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
        Sample<?> sample = (Sample<?>) o;
        return Objects.equals( instant, sample.instant ) && Objects.equals( value, sample.value );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( instant, value );
    }

    @Override
    public String toString()
    {
        return "Sample{" + "instant=" + instant + ", value=" + value + '}';
    }
}
