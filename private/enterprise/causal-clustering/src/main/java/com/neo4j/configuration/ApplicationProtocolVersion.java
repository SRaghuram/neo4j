/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import io.netty.buffer.ByteBuf;

import org.neo4j.annotations.api.PublicApi;

import static java.util.Objects.hash;
import static org.neo4j.util.Preconditions.requireNonNegative;

@PublicApi
public class ApplicationProtocolVersion implements Comparable<ApplicationProtocolVersion>
{
    private final int major;
    private final int minor;

    public ApplicationProtocolVersion( int major, int minor )
    {
        this.major = requireNonNegative( major );
        this.minor = requireNonNegative( minor );
    }

    public int major()
    {
        return major;
    }

    public int minor()
    {
        return minor;
    }

    public static ApplicationProtocolVersion parse( String value )
    {
        var split = value.split( "\\." );
        if ( split.length != 2 )
        {
            throw illegalStringValue( value );
        }

        try
        {
            var major = Integer.parseInt( split[0] );
            var minor = Integer.parseInt( split[1] );
            return new ApplicationProtocolVersion( major, minor );
        }
        catch ( NumberFormatException e )
        {
            throw illegalStringValue( value );
        }
    }

    public static ApplicationProtocolVersion decode( ByteBuf buf )
    {
        return new ApplicationProtocolVersion( buf.readInt(), buf.readInt() );
    }

    public void encode( ByteBuf buf )
    {
        buf.writeInt( major );
        buf.writeInt( minor );
    }

    @Override
    public int compareTo( ApplicationProtocolVersion other )
    {
        var majorComparison = Integer.compare( major, other.major );
        if ( majorComparison == 0 )
        {
            return Integer.compare( minor, other.minor );
        }
        return majorComparison;
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
        ApplicationProtocolVersion that = (ApplicationProtocolVersion) o;
        return major == that.major &&
               minor == that.minor;
    }

    @Override
    public int hashCode()
    {
        return hash( major, minor );
    }

    @Override
    public String toString()
    {
        return major + "." + minor;
    }

    private static RuntimeException illegalStringValue( String value )
    {
        return new IllegalArgumentException( "Illegal protocol version string: " + value + ". " +
                                             "Expected a value in format '<major>.<minor>' where both major and minor are non-negative integer values" );
    }
}
