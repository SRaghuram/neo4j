/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.enterprise.builtinprocs;

public class ConnectionTerminationFailedResult extends ConnectionTerminationResult
{
    private static final String UNKNOWN_USER = "n/a";
    private static final String FAILURE_MESSAGE = "No connection found with this id";

    ConnectionTerminationFailedResult( String connectionId )
    {
        super( connectionId, UNKNOWN_USER, FAILURE_MESSAGE );
    }
}
