/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.options;

public class Version
{
    private String mainVersion;
    private String minorVersion;
    private String patchVersion;
    private String preReleaseBranch;

    public Version( String versionString )
    {
        String[] split = versionString.split( "\\." );

        if ( split.length != 3 )
        {
            throw new IllegalArgumentException( String.format( "Neo4j version have always been on the form x.y.z , but this version is %s", versionString ) );
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

    public String fullVersion()
    {
        if ( preReleaseBranch == null )
        {
            return patchVersion();
        }
        return String.format( "%s.%s.%s-%s", mainVersion, minorVersion, patchVersion, preReleaseBranch );
    }

    public static String toSanitizeVersion( String version )
    {
        return new Version( version ).patchVersion();
    }

    @Override
    public String toString()
    {
        return fullVersion();
    }
}
