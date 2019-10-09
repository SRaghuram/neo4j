/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import org.neo4j.kernel.api.exceptions.Status;

public class FabricException extends RuntimeException implements Status.HasStatus
{
    private final Status statusCode;

    public FabricException( Status statusCode, Throwable cause )
    {
        super( cause );
        this.statusCode = statusCode;
    }

    public FabricException( Status statusCode, String message, Object... parameters )
    {
        super( String.format( message, parameters ) );
        this.statusCode = statusCode;
    }

    public FabricException( Status statusCode, String message, Throwable cause )
    {
        super( message, cause );
        this.statusCode = statusCode;
    }

    @Override
    public Status status()
    {
        return statusCode;
    }
}
