/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

public class DiscoveryTimeoutException extends Exception
{
    public DiscoveryTimeoutException()
    {
        super();
    }

    public DiscoveryTimeoutException( String message )
    {
        super( message );
    }

    public DiscoveryTimeoutException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public DiscoveryTimeoutException( Throwable cause )
    {
        super( cause );
    }

    protected DiscoveryTimeoutException( String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace )
    {
        super( message, cause, enableSuppression, writableStackTrace );
    }
}
