/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.neo4j.kernel.impl.transaction.log.checkpoint.AbstractCheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;

public class VolumetricCheckPointThreshold extends AbstractCheckPointThreshold
{
    private final LogPruning logPruning;

    public VolumetricCheckPointThreshold( LogPruning logPruning )
    {
        super( "tx log pruning" );
        this.logPruning = logPruning;
    }

    @Override
    protected boolean thresholdReached( long lastCommittedTransactionId )
    {
        return logPruning.mightHaveLogsToPrune();
    }

    @Override
    public void initialize( long transactionId )
    {
    }

    @Override
    public void checkPointHappened( long transactionId )
    {
    }

    @Override
    public long checkFrequencyMillis()
    {
        return DEFAULT_CHECKING_FREQUENCY_MILLIS;
    }
}
