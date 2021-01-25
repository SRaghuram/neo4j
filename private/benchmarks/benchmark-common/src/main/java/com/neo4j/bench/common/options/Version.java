/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.options;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Version
{
    private String mainVersion;
    private String minorVersion;
    private String patchVersion;
    private String preReleaseBranch;

    @JsonCreator
    public Version( String versionString )
    {
        String[] split = versionString.split( "\\." );

        if ( split.length != 4 && split.length != 3 )
        {
            throw new IllegalArgumentException(
                    String.format( "Expected Neo4j version of the form x.y.z(-dropxy.z) , but got %s", versionString ) );
        }

        String[] patchAndPreReleaseBranch = split[2].split( "-" );
        if ( patchAndPreReleaseBranch.length != 1 && patchAndPreReleaseBranch.length != 2 )
        {
            throw new IllegalArgumentException( String.format( "Neo4j version have always been on the form x.y.z , but this version is %s", versionString ) );
        }

        mainVersion = ensureValidVersionNumber( split[0] );
        minorVersion = ensureValidVersionNumber( split[1] );
        patchVersion = ensureValidVersionNumber( patchAndPreReleaseBranch[0] );
        if ( patchAndPreReleaseBranch.length > 1 )
        {
            preReleaseBranch = patchAndPreReleaseBranch[1];
            if ( split.length == 4 )
            {
                preReleaseBranch += "." + ensureValidVersionNumber( split[3] );
            }
        }
    }

    private String ensureValidVersionNumber( String current )
    {
        if ( !current.matches( "[0-9]+" ) )
        {
            throw new IllegalArgumentException( String.format( "Neo4j version should be numbers, but found %s", current ) );
        }
        return current;
    }

    public String mainVersion()
    {
        return mainVersion;
    }

    public String minorVersion()
    {
        return String.format( "%s.%s", mainVersion, minorVersion );
    }

    public String patchVersion()
    {
        return String.format( "%s.%s.%s", mainVersion, minorVersion, patchVersion );
    }

    @JsonValue
    public String fullVersion()
    {
        if ( preReleaseBranch == null )
        {
            return patchVersion();
        }
        return String.format( "%s.%s.%s-%s", mainVersion, minorVersion, patchVersion, preReleaseBranch );
    }

    @Override
    public String toString()
    {
        return fullVersion();
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

        Version that = (Version) o;

        return EqualsBuilder.reflectionEquals( this, that );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }
}
