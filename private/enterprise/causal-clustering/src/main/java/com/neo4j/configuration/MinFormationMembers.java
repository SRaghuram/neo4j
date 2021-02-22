/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.util.Objects;

import org.neo4j.configuration.Config;

public class MinFormationMembers
{
    private final int value;

    public static MinFormationMembers from( Config config )
    {
        var value = config.get( CausalClusteringInternalSettings.middleware_akka_min_number_of_members_at_formation );
        return new MinFormationMembers( value );
    }

    public MinFormationMembers( int value )
    {
        this.value = value;
    }

    public int value()
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
        if ( !(o instanceof MinFormationMembers) )
        {
            return false;
        }
        MinFormationMembers that = (MinFormationMembers) o;
        return value == that.value;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( value );
    }

    @Override
    public String toString()
    {
        return "MinFormationMembers{" +
               "value=" + value +
               '}';
    }
}
