/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.CoreTopologyService.Listener;
import com.neo4j.causalclustering.identity.RaftId;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoreTopologyListenerServiceTest
{
    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final DatabaseId id1 = databaseIdRepository.getByName( "database_one" ).get();
    private final DatabaseId id2 = databaseIdRepository.getByName( "database_two" ).get();

    private final CoreTopologyListenerService listenerService = new CoreTopologyListenerService();

    @Test
    void shouldNotifyListeners()
    {
        Listener listener1 = newListenerMock( id1 );
        Listener listener2 = newListenerMock( id1 );
        Listener listener3 = newListenerMock( id2 );

        listenerService.addCoreTopologyListener( listener1 );
        listenerService.addCoreTopologyListener( listener2 );
        listenerService.addCoreTopologyListener( listener3 );

        DatabaseCoreTopology coreTopology1 = new DatabaseCoreTopology( id1, RaftId.from( id1 ), Map.of() );
        DatabaseCoreTopology coreTopology2 = new DatabaseCoreTopology( id2, RaftId.from( id2 ), Map.of() );

        listenerService.notifyListeners( coreTopology1 );
        listenerService.notifyListeners( coreTopology2 );

        verify( listener1 ).onCoreTopologyChange( coreTopology1 );
        verify( listener2 ).onCoreTopologyChange( coreTopology1 );
        verify( listener3, never() ).onCoreTopologyChange( coreTopology1 );

        verify( listener1, never() ).onCoreTopologyChange( coreTopology2 );
        verify( listener2, never() ).onCoreTopologyChange( coreTopology2 );
        verify( listener3 ).onCoreTopologyChange( coreTopology2 );
    }

    @Test
    void shouldNotNotifyRemovedListeners()
    {
        Listener listener1 = newListenerMock( id1 );
        Listener listener2 = newListenerMock( id1 );

        listenerService.addCoreTopologyListener( listener1 );
        listenerService.addCoreTopologyListener( listener2 );
        listenerService.removeCoreTopologyListener( listener1 );

        DatabaseCoreTopology coreTopology = new DatabaseCoreTopology( id1, RaftId.from( id1 ), Map.of() );

        listenerService.notifyListeners( coreTopology );

        verify( listener1, never() ).onCoreTopologyChange( any() );
        verify( listener2 ).onCoreTopologyChange( coreTopology );
    }

    private static Listener newListenerMock( DatabaseId databaseId )
    {
        Listener listener = mock( Listener.class );
        when( listener.databaseId() ).thenReturn( databaseId );
        return listener;
    }
}
