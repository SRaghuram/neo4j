/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import java.util.Collection;

class CompositeAsyncEventHandler implements AsyncTaskEventHandler
{
    private final Collection<AsyncTaskEventHandler> asyncTaskEventHandlerList;

    CompositeAsyncEventHandler( Collection<AsyncTaskEventHandler> asyncTaskEventHandlerList )
    {
        this.asyncTaskEventHandlerList = asyncTaskEventHandlerList;
    }

    @Override
    public void onFailure( Exception e )
    {
        asyncTaskEventHandlerList.forEach( eh -> eh.onFailure( e ) );
    }

    @Override
    public void onSuccess()
    {
        asyncTaskEventHandlerList.forEach( AsyncTaskEventHandler::onSuccess );
    }
}
