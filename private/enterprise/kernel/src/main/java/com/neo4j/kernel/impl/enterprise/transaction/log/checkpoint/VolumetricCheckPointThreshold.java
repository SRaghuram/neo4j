/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint;

import org.neo4j.kernel.impl.transaction.log.checkpoint.AbstractCheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;

public class VolumetricCheckPointThreshold extends AbstractCheckPointThreshold
{
    private final LogPruning logPruning;

    VolumetricCheckPointThreshold( LogPruning logPruning )
    {
        super( "volumetric checkpoint threshold, based on log pruning strategy" );
        this.logPruning = logPruning;
    }

    @Override
    protected String createCheckpointThresholdDescription( String description )
    {
        // Always build a new description every time, since the log pruning strategy can change dynamically.
        return description + " '" + logPruning.describeCurrentStrategy() + "'";
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
