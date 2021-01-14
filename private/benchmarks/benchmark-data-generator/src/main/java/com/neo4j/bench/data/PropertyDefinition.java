/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import java.util.Objects;

import static java.lang.String.format;

public class PropertyDefinition
{
    private String key;
    private ValueGeneratorFactory valueGeneratorFactory;

    private PropertyDefinition()
    {
    }

    public PropertyDefinition( String key, ValueGeneratorFactory valueGeneratorFactory )
    {
        this.key = key;
        this.valueGeneratorFactory = valueGeneratorFactory;
    }

    public String key()
    {
        return key;
    }

    public ValueGeneratorFactory value()
    {
        return valueGeneratorFactory;
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
        PropertyDefinition that = (PropertyDefinition) o;
        return Objects.equals( key, that.key ) &&
               valueGeneratorFactory.equals( that.valueGeneratorFactory );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( key, valueGeneratorFactory );
    }

    @Override
    public String toString()
    {
        return format( "(%s,%s)", key, valueGeneratorFactory.getClass().getSimpleName() );
    }
}
