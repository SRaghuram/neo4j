/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.database.DatabaseId;

public class CoreTopologyListenerService
{
    private final Set<CoreTopologyService.Listener> listeners;

    public CoreTopologyListenerService()
    {
        this.listeners = ConcurrentHashMap.newKeySet();
    }

    public void addCoreTopologyListener( CoreTopologyService.Listener listener )
    {
        listeners.add( listener );
    }

    public void removeCoreTopologyListener( CoreTopologyService.Listener listener )
    {
        listeners.remove( listener );
    }

    public void notifyListeners( CoreTopology coreTopology )
    {
        for ( CoreTopologyService.Listener listener : listeners )
        {
            DatabaseId databaseId = listener.databaseId();

            listener.onCoreTopologyChange( coreTopology.filterTopologyByDb( databaseId ) );
        }
    }
}
