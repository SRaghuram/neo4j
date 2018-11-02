/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

public class CatchUpClientException extends Exception
{
    CatchUpClientException( String message )
    {
        super( message );
    }

    CatchUpClientException( String operation, Throwable cause )
    {
        super( operation, cause );
    }
}
