/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

public class NoLeaderFoundException extends Exception
{
    public NoLeaderFoundException()
    {
    }

    public NoLeaderFoundException( Throwable cause )
    {
        super( cause );
    }

    public NoLeaderFoundException( String message )
    {
        super( message );
    }

    public NoLeaderFoundException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
