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

    public Version( String versionString )
    {
        String[] split = versionString.split( "\\." );
        if ( split.length != 3 )
        {
            throw new IllegalArgumentException( String.format( "Neo4j version have always been on the form x.xx.xx , but this version is %s", versionString ) );
        }
        mainVersion = split[0];
        minorVersion = split[1];
        patchVersion = split[2];
    }

    public String getMainVersion()
    {
        return mainVersion;
    }

    public String getMinorVersion()
    {
        return minorVersion;
    }

    public String getPatchVersion()
    {
        return patchVersion;
    }

    public String getMainAndMinorAndPatchVersion()
    {
        return String.format( "%s.%s.%s", mainVersion, minorVersion, patchVersion );
    }

    public String getMainAndMinorVersion()
    {
        return String.format( "%s.%s", mainVersion, minorVersion );
    }
}
