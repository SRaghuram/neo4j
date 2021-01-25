/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.txtracking;

import org.neo4j.bolt.txtracking.TransactionIdTrackerMonitor;

public class WaitTrackingMonitor implements TransactionIdTrackerMonitor
{
    private volatile boolean waiting;

    @Override
    public void onWaitWhenNotUpToDate()
    {
        waiting = true;
    }

    public boolean isWaiting()
    {
        return waiting;
    }

    public void clearWaiting()
    {
        waiting = false;
    }
}
