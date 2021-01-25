/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

public class DatabaseIdDownloadFailedException extends Exception
{
    public DatabaseIdDownloadFailedException( Exception e )
    {
        super( e );
    }

    public DatabaseIdDownloadFailedException( String description )
    {
        super( description );
    }

    public DatabaseIdDownloadFailedException( String msg, Exception ex )
    {
        super( msg, ex );
    }
}
