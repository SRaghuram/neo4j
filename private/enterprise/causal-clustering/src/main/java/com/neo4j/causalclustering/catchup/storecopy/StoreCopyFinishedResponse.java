/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.util.Objects;

public class StoreCopyFinishedResponse
{
    public static final long LAST_CHECKPOINTED_TX_UNAVAILABLE = -1L;

    public enum Status
    {
        SUCCESS,
        E_STORE_ID_MISMATCH,
        E_TOO_FAR_BEHIND,
        E_UNKNOWN,
        E_DATABASE_UNKNOWN
    }

    private final Status status;
    private final long lastCheckpointedTx;

    /**
     * @param status provides the response status
     * @param lastCheckpointedTx lastCheckpointedTx. This may be unavailable to the decoder for two reason.
     * 1 if using an older catchup protocol
     * 2. and error occurred while responding.
     * If not available, it should be -1.
     */
    public StoreCopyFinishedResponse( Status status, long lastCheckpointedTx )
    {
        this.status = status;
        this.lastCheckpointedTx = status == Status.SUCCESS ? lastCheckpointedTx : LAST_CHECKPOINTED_TX_UNAVAILABLE;
    }

    public Status status()
    {
        return status;
    }

    /**
     * Only available on V3 of catchup protocol.
     * @return last checkpointed tx after file send is complete if available. Otherwise -1.
     */
    public long lastCheckpointedTx()
    {
        return lastCheckpointedTx;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        StoreCopyFinishedResponse that = (StoreCopyFinishedResponse) o;
        return lastCheckpointedTx == that.lastCheckpointedTx && status == that.status;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( status, lastCheckpointedTx );
    }
}
