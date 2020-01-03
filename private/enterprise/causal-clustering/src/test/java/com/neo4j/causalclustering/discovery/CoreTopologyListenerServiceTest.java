/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.CoreTopologyService.Listener;
import com.neo4j.causalclustering.identity.RaftId;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoreTopologyListenerServiceTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final NamedDatabaseId id1 = databaseIdRepository.getRaw( "database_one" );
    private final NamedDatabaseId id2 = databaseIdRepository.getRaw( "database_two" );

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

        DatabaseCoreTopology coreTopology1 = new DatabaseCoreTopology( id1.databaseId(), RaftId.from( id1.databaseId() ), Map.of() );
        DatabaseCoreTopology coreTopology2 = new DatabaseCoreTopology( id2.databaseId(), RaftId.from( id2.databaseId() ), Map.of() );

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

        DatabaseCoreTopology coreTopology = new DatabaseCoreTopology( id1.databaseId(), RaftId.from( id1.databaseId() ), Map.of() );

        listenerService.notifyListeners( coreTopology );

        verify( listener1, never() ).onCoreTopologyChange( any() );
        verify( listener2 ).onCoreTopologyChange( coreTopology );
    }

    private static Listener newListenerMock( NamedDatabaseId namedDatabaseId )
    {
        Listener listener = mock( Listener.class );
        when( listener.namedDatabaseId() ).thenReturn( namedDatabaseId );
        return listener;
    }
}
