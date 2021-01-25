/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

public class StoreIdDownloadFailedException extends Exception
{
    public StoreIdDownloadFailedException( Throwable cause )
    {
        super( cause );
    }

    public StoreIdDownloadFailedException( String message )
    {
        super( message );
    }

    public StoreIdDownloadFailedException( String msg, Exception ex )
    {
        super( msg, ex );
    }
}
