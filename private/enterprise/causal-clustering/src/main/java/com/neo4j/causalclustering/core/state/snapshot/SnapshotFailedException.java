/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

public class SnapshotFailedException extends Exception
{
    /**
     * Status that determines how the snapshot downloader should handle the exception
     * RETRYABLE - The current snapshot download operation can immediately be retried
     * TERMINAL - The current snapshot download operation should stop
     * UNRECOVERABLE - The current error cannot progress and will never be able to progress. This suggest something is wrong within the cluster.
     */
    enum Status
    {
        RETRYABLE,
        TERMINAL,
        UNRECOVERABLE;
    }

    private final Status status;

    SnapshotFailedException( String message, Status status, Throwable cause )
    {
        super( message( message, status ), cause );
        this.status = status;
    }

    SnapshotFailedException( String message, Status status )
    {
        super( message( message, status ) );
        this.status = status;
    }

    Status status()
    {
        return status;
    }

    private static String message( String message, Status status )
    {
        return String.format( "Downloading of snapshot failed. [ErrorStatus: %s], [Message: %s]", status.name(), message );
    }
}
