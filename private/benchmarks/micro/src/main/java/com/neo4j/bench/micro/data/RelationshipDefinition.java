/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import java.util.Objects;
import java.util.stream.Stream;

import org.neo4j.graphdb.RelationshipType;

public class RelationshipDefinition
{
    private final RelationshipType type;
    private final int count;

    public RelationshipDefinition( RelationshipType type, int count )
    {
        this.type = type;
        this.count = count;
    }

    public RelationshipType type()
    {
        return type;
    }

    public int count()
    {
        return count;
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
        RelationshipDefinition that = (RelationshipDefinition) o;
        return count == that.count && type.equals( that.type );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( type, count );
    }

    @Override
    public String toString()
    {
        return "(" + type + "," + count + ")";
    }

    public static RelationshipDefinition[] from( String s )
    {
        return Stream.of( s.split( "," ) )
                .map( RelationshipDefinition::definitionFrom )
                .toArray( RelationshipDefinition[]::new );
    }

    private static RelationshipDefinition definitionFrom( String s )
    {
        RelationshipType type = RelationshipType.withName( s.substring( 1, s.indexOf( ':' ) ) );
        int count = Integer.parseInt( s.substring( s.indexOf( ':' ) + 1, s.length() - 1 ) );
        return new RelationshipDefinition( type, count );
    }
}
