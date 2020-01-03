/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    public void notifyListeners( DatabaseCoreTopology coreTopology )
    {
        for ( CoreTopologyService.Listener listener : listeners )
        {
            if ( listener.namedDatabaseId().databaseId().equals( coreTopology.databaseId() ) )
            {
                listener.onCoreTopologyChange( coreTopology );
            }
        }
    }
}
