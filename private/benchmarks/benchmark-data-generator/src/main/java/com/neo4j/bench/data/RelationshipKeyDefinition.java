/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.data;

import java.util.Objects;

import org.neo4j.graphdb.RelationshipType;

import static java.lang.String.format;

public class RelationshipKeyDefinition
{
    private String typeName;
    private String key;

    public RelationshipKeyDefinition()
    {
    }

    public RelationshipKeyDefinition( RelationshipType relationshipType, String key )
    {
        this.typeName = relationshipType.name();
        this.key = key;
    }

    public RelationshipType type()
    {
        return RelationshipType.withName( typeName );
    }

    public String key()
    {
        return key;
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
        RelationshipKeyDefinition that = (RelationshipKeyDefinition) o;
        return Objects.equals( typeName, that.typeName ) &&
               Objects.equals( key, that.key );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( typeName, key );
    }

    @Override
    public String toString()
    {
        return format( "(:%s,%s)", typeName, key );
    }
}
