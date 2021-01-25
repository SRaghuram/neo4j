/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import java.io.IOException;

/**
 * Throwing an instance of this exception indicates that the creation or handling of a message failed because of
 * size restrictions.
 */
public class MessageTooBigException extends IOException
{
    public MessageTooBigException( String message )
    {
        super( message );
    }
}
