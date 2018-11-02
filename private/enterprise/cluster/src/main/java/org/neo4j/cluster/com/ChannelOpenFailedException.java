/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.com;

/**
 * This is thrown when {@link NetworkSender} is unable to open a channel to another instance
 * in the cluster.
 */
public class ChannelOpenFailedException extends RuntimeException
{
    public ChannelOpenFailedException()
    {
        super();
    }

    public ChannelOpenFailedException( String message )
    {
        super( message );
    }

    public ChannelOpenFailedException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public ChannelOpenFailedException( Throwable cause )
    {
        super( cause );
    }
}
