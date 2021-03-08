/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.database;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.lang.String.format;

public class AllSupportedVersions
{

    private static final List<String> VERSIONS = ImmutableList.of( "3.5", "4.0", "4.1", "4.2", "4.3" );

    public static String prevVersion( String version )
    {
        int index = VERSIONS.indexOf( version );
        if ( index == -1 || index == 0 )
        {
            throw new IllegalArgumentException( format( "unsupported or not found previous version of %s, please update %s class",
                                                        version,
                                                        AllSupportedVersions.class.getName() ) );
        }
        return VERSIONS.get( index - 1 );
    }
}
