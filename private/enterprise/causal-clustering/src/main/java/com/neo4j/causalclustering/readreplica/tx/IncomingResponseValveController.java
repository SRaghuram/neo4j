/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import com.neo4j.causalclustering.catchup.IncomingResponseValve;

class IncomingResponseValveController implements AsyncTaskEventHandler
{
    private long currentValue;
    private final long upperLimit;
    private final long lowerLimit;
    private final IncomingResponseValve incomingResponseValve;

    IncomingResponseValveController( long upperLimit, long lowerLimit, IncomingResponseValve incomingResponseValve )
    {
        if ( upperLimit <= lowerLimit )
        {
            throw new IllegalArgumentException( "Upper watermark must be above lower watermark" );
        }
        this.lowerLimit = lowerLimit;
        this.incomingResponseValve = incomingResponseValve;
        this.upperLimit = upperLimit;
    }

    synchronized void increment()
    {
        if ( ++currentValue == upperLimit )
        {
            incomingResponseValve.shut();
        }
    }

    private synchronized void decrement()
    {
        if ( --currentValue == lowerLimit )
        {
            incomingResponseValve.open();
        }
    }

    @Override
    public void onFailure( Exception e )
    {
        decrement();
    }

    @Override
    public void onSuccess()
    {
        decrement();
    }
}
