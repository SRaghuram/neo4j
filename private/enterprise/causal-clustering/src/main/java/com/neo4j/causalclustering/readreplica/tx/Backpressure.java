/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.FlowControl;

class Backpressure implements AsyncTaskEventHandler
{
    private long localAsyncQueue;
    private final long upperWatermark;
    private final long lowerWatermark;
    private final FlowControl flowControl;

    Backpressure( long upperWatermark, long lowerWatermark, FlowControl flowControl )
    {
        if ( upperWatermark <= lowerWatermark )
        {
            throw new IllegalArgumentException( "Upper watermark must be above lower watermark" );
        }
        this.lowerWatermark = lowerWatermark;
        this.flowControl = flowControl;
        this.upperWatermark = upperWatermark;
    }

    synchronized void scheduledJob()
    {
        if ( ++localAsyncQueue == upperWatermark )
        {
            flowControl.stopReading();
        }
    }

    private synchronized void release()
    {
        if ( --localAsyncQueue == lowerWatermark )
        {
            flowControl.continueReading();
        }
    }

    @Override
    public void onFailure( Exception e )
    {
        release();
    }

    @Override
    public void onSuccess()
    {
        release();
    }
}
