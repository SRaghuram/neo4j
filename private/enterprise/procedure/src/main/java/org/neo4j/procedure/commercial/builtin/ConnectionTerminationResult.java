/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure.commercial.builtin;

public class ConnectionTerminationResult
{
    private static final String SUCCESS_MESSAGE = "Connection found";

    public final String connectionId;
    public final String username;
    public final String message;

    ConnectionTerminationResult( String connectionId, String username )
    {
        this( connectionId, username, SUCCESS_MESSAGE );
    }

    ConnectionTerminationResult( String connectionId, String username, String message )
    {
        this.connectionId = connectionId;
        this.username = username;
        this.message = message;
    }
}
