/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import java.util.Collection;

class CompositeFailureEventHandler implements FailureEventHandler
{
    private final Collection<FailureEventHandler> failureEventHandlerList;

    CompositeFailureEventHandler( Collection<FailureEventHandler> failureEventHandlerList )
    {
        this.failureEventHandlerList = failureEventHandlerList;
    }

    @Override
    public void onFailure( Exception e )
    {
        failureEventHandlerList.forEach( feh -> feh.onFailure( e ) );
    }
}
