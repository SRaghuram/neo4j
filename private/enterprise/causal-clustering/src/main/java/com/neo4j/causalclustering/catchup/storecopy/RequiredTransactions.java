/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import static org.neo4j.internal.helpers.collection.LongRange.assertIsRange;

public class RequiredTransactions
{
    private static final long NO_REQUIRED_TX_ID = -1;
    private final long startTxId;
    private final long requiredTx;

    static RequiredTransactions requiredRange( long startTxId, long requiredTxId )
    {
        assertIsRange( startTxId, requiredTxId );
        return new RequiredTransactions( startTxId, requiredTxId );
    }

    static RequiredTransactions noConstraint( long myHighestTxId )
    {
        return new RequiredTransactions( myHighestTxId, NO_REQUIRED_TX_ID );
    }

    private RequiredTransactions( long startTxId, long requiredTx )
    {
        if ( startTxId < 0 )
        {
            throw new IllegalArgumentException( "Start tx id cannot be negative. Got: " + startTxId );
        }
        this.startTxId = startTxId;
        this.requiredTx = requiredTx;
    }

    public long startTxId()
    {
        return startTxId;
    }

    boolean noRequiredTxId()
    {
        return requiredTx == NO_REQUIRED_TX_ID;
    }

    long requiredTxId()
    {
        return requiredTx;
    }

    @Override
    public String toString()
    {
        return "RequiredTransactions{" + "from=" + startTxId + ", to=" + requiredTx + '}';
    }
}
