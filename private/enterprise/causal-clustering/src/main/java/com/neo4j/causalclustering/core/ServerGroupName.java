/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

import org.neo4j.configuration.SettingValueParser;

import static org.neo4j.configuration.SettingValueParsers.STRING;

public class ServerGroupName implements CharSequence, Comparable<ServerGroupName>
{
    public static final ServerGroupName EMPTY = new ServerGroupName( "" );
    private final String value;

    public ServerGroupName( @Nonnull String name )
    {
        this.value = name;
    }

    public String getRaw()
    {
        return value;
    }

    public static List<ServerGroupName> listOf( String... names )
    {
        return List.of( Arrays.stream( names ).map( ServerGroupName::new ).toArray( ServerGroupName[]::new ) );
    }

    public static Set<ServerGroupName> setOf( String... names )
    {
        return setOf( Arrays.asList( names ) );
    }

    public static Set<ServerGroupName> setOf( Collection<String> names )
    {
        var deduplicatedNames = names.stream().map( ServerGroupName::new ).collect( Collectors.toSet() );
        return Set.of( deduplicatedNames.toArray( ServerGroupName[]::new ) );
    }

    public static final SettingValueParser<ServerGroupName> SERVER_GROUP_NAME = new SettingValueParser<>()
    {
        @Override
        public ServerGroupName parse( String value )
        {
            return new ServerGroupName( STRING.parse( value ) );
        }

        @Override
        public String getDescription()
        {
            return "a string identifying a Server Group";
        }

        @Override
        public Class<ServerGroupName> getType()
        {
            return ServerGroupName.class;
        }
    };

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
        var that = (ServerGroupName) o;
        return value.equals( that.value );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( value );
    }

    @Override
    public int length()
    {
        return value.length();
    }

    @Override
    public char charAt( int index )
    {
        return value.charAt( index );
    }

    @Override
    public CharSequence subSequence( int start, int end )
    {
        return value.subSequence( start, end );
    }

    @Override
    public String toString()
    {
        return value;
    }

    @Override
    public IntStream chars()
    {
        return value.chars();
    }

    @Override
    public IntStream codePoints()
    {
        return value.codePoints();
    }

    @Override
    public int compareTo( ServerGroupName o )
    {
        return this.value.compareTo( o.value );
    }
}
