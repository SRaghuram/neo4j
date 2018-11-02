/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import static java.lang.String.format;

public class CatchUpProtocolViolationException extends Exception
{
    public CatchUpProtocolViolationException( String message, Object... args )
    {
        super( format( message, args ) );
    }
}
