/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

public class StoreCopyFailedException extends Exception
{
    public StoreCopyFailedException( Throwable cause )
    {
        super( cause );
    }

    public StoreCopyFailedException( String message )
    {
        super( message );
    }

    public StoreCopyFailedException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
