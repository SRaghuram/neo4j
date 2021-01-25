/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

public class CatchUpClientException extends Exception
{
    public CatchUpClientException()
    {
        super();
    }

    CatchUpClientException( String message )
    {
        super( message );
    }

    CatchUpClientException( String operation, Throwable cause )
    {
        super( operation, cause );
    }
}
