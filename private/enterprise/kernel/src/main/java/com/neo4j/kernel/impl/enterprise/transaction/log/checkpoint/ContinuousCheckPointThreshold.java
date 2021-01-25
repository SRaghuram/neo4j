/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.neo4j.kernel.impl.transaction.log.checkpoint.AbstractCheckPointThreshold;

class ContinuousCheckPointThreshold extends AbstractCheckPointThreshold
{
    private volatile long nextTransactionIdTarget;

    ContinuousCheckPointThreshold()
    {
        super( "continuous threshold" );
    }

    @Override
    protected boolean thresholdReached( long lastCommittedTransactionId, long lastCommittedTransactionLogVersion )
    {
        return lastCommittedTransactionId >= nextTransactionIdTarget;
    }

    @Override
    public void initialize( long transactionId )
    {
        checkPointHappened( transactionId );
    }

    @Override
    public void checkPointHappened( long transactionId )
    {
        nextTransactionIdTarget = transactionId + 1;
    }

    @Override
    public long checkFrequencyMillis()
    {
        return 100;
    }
}
