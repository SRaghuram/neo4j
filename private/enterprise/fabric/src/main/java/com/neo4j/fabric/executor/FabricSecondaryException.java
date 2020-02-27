/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import org.neo4j.kernel.api.exceptions.Status;

/**
 * A Fabric exception that is not the primary cause of a failure.
 * <p>
 * Fabric concurrent parts are not independent. A failure in one record stream can cause secondary failures in other streams for instance
 * if the streams are using the same transaction. Due to the concurrent nature, there might be a race between the primary exception and secondary exceptions.
 * The user should be always presented with the primary exception.
 */
public class FabricSecondaryException extends FabricException
{
    private final FabricException primaryException;

    public FabricSecondaryException( Status statusCode, String message, Throwable cause, FabricException primaryException )
    {
        super( statusCode, message, cause );
        this.primaryException = primaryException;
    }

    public FabricException getPrimaryException()
    {
        return primaryException;
    }
}
