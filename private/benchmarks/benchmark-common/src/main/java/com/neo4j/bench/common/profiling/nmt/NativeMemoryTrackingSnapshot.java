/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.nmt;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NativeMemoryTrackingSnapshot
{
    private static final Pattern SNAPSHOT_TEMPLATE = Pattern.compile( "nmt_(?<counter>\\d+).snapshot" );

    public static String snapshotFilename( long counter )
    {
        return String.format( "nmt_%d.snapshot", counter );
    }

    public static boolean matches( String snapshotFilename )
    {
        return SNAPSHOT_TEMPLATE.matcher( snapshotFilename ).matches();
    }

    public static long counter( String snapshotFilename )
    {
        Matcher matcher = SNAPSHOT_TEMPLATE.matcher( snapshotFilename );
        if ( matcher.matches() )
        {
            String counter = matcher.group( "counter" );
            return Long.parseLong( counter );
        }
        throw new IllegalArgumentException( String.format( "snapshot filename %s is not parsable", snapshotFilename ) );
    }

}
