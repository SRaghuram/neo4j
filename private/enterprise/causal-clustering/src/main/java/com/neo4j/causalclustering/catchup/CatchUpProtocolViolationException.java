/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import static java.lang.String.format;

public class CatchUpProtocolViolationException extends Exception
{
    public CatchUpProtocolViolationException( String message, Object... args )
    {
        super( format( message, args ) );
    }
}
