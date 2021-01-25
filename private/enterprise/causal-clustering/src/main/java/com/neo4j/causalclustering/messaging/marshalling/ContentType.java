/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

public enum ContentType
{
    ContentType( (byte) 0 ),
    ReplicatedContent( (byte) 1 ),
    RaftLogEntryTerms( (byte) 2 ),
    Message( (byte) 3 );

    private final byte messageCode;

    ContentType( byte messageCode )
    {
        this.messageCode = messageCode;
    }

    public byte get()
    {
        return messageCode;
    }
}
