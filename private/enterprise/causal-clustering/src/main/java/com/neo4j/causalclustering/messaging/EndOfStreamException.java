/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

/**
 * An exception which works around the fact that ReadPastEndException inherits from
 * IOException even though the handling of an end of stream situation in general is
 * recoverable and must be handled explicitly.
 */
public class EndOfStreamException extends Exception
{
    public EndOfStreamException( Throwable e )
    {
        this.initCause( e );
    }
}
