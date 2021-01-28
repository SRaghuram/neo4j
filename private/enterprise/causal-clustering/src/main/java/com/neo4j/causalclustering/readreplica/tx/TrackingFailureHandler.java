/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

class TrackingFailureHandler implements FailureEventHandler, Aborter
{
    private volatile boolean hasFailed;

    @Override
    public void onFailure( Exception e )
    {
        hasFailed = true;
    }

    @Override
    public boolean shouldAbort()
    {
        return hasFailed;
    }
}
