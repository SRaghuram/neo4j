/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import org.neo4j.kernel.api.exceptions.Status;

public class AcquireBarrierTimeoutException extends RuntimeException implements Status.HasStatus
{
    private final Status status;

    public AcquireBarrierTimeoutException( Throwable cause, String message, Status status )
    {
        super( message, cause );
        this.status = status;
    }

    public AcquireBarrierTimeoutException( String message, Status status )
    {
        super( message );
        this.status = status;
    }

    @Override
    public Status status()
    {
        return status;
    }
}
