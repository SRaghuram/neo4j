/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

public class S3Util
{
    public static void assertSaneS3Path( String... s3Paths )
    {
        for ( String s3Path : s3Paths )
        {
            if ( s3Path.startsWith( "http://" ) || s3Path.startsWith( "s3://" ) )
            {
                throw new RuntimeException( "S3 path should not include protocol: " + s3Path );
            }
            if ( !s3Path.contains( "/" ) )
            {
                throw new RuntimeException( "S3 path must begin with '<bucket>/': " + s3Path );
            }

        }
    }
}
