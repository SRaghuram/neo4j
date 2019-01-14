/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

/**
 * Exception indicating that a store is not fit for participating in a particular cluster. It might have diverged, be
 * to old or otherwise unfit for a cluster. This does not mean however that it is corrupt or not in some way suitable
 * for standalone use.
 */
public class StoreUnableToParticipateInClusterException extends IllegalStateException
{
    public StoreUnableToParticipateInClusterException()
    {
        super();
    }

    public StoreUnableToParticipateInClusterException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public StoreUnableToParticipateInClusterException( String message )
    {
        super( message );
    }

    public StoreUnableToParticipateInClusterException( Throwable cause )
    {
        super( cause );
    }
}
