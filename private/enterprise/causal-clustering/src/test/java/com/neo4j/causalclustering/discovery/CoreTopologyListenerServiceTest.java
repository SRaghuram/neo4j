/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.CoreTopologyService.Listener;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

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
        var listener1 = newListenerMock( id1 );
        var listener2 = newListenerMock( id1 );
        var listener3 = newListenerMock( id2 );

        listenerService.addCoreTopologyListener( listener1 );
        listenerService.addCoreTopologyListener( listener2 );
        listenerService.addCoreTopologyListener( listener3 );

        var databaseRaftMembers1 = createTopologyAndGetMembers( id1 );
        var databaseRaftMembers2 = createTopologyAndGetMembers( id2 );

        listenerService.notifyListeners( id1.databaseId(), databaseRaftMembers1 );
        listenerService.notifyListeners( id2.databaseId(), databaseRaftMembers2 );

        verify( listener1 ).onCoreTopologyChange( databaseRaftMembers1 );
        verify( listener2 ).onCoreTopologyChange( databaseRaftMembers1 );
        verify( listener3, never() ).onCoreTopologyChange( databaseRaftMembers1 );

        verify( listener1, never() ).onCoreTopologyChange( databaseRaftMembers2 );
        verify( listener2, never() ).onCoreTopologyChange( databaseRaftMembers2 );
        verify( listener3 ).onCoreTopologyChange( databaseRaftMembers2 );
    }

    @Test
    void shouldNotNotifyRemovedListeners()
    {
        var listener1 = newListenerMock( id1 );
        var listener2 = newListenerMock( id1 );

        listenerService.addCoreTopologyListener( listener1 );
        listenerService.addCoreTopologyListener( listener2 );
        listenerService.removeCoreTopologyListener( listener1 );

        var databaseRaftMembers = createTopologyAndGetMembers( id1 );

        listenerService.notifyListeners( id1.databaseId(), databaseRaftMembers );

        verify( listener1, never() ).onCoreTopologyChange( any() );
        verify( listener2 ).onCoreTopologyChange( databaseRaftMembers );
    }

    private static Listener newListenerMock( NamedDatabaseId namedDatabaseId )
    {
        var listener = mock( Listener.class );
        when( listener.namedDatabaseId() ).thenReturn( namedDatabaseId );
        return listener;
    }

    private static Set<RaftMemberId> createTopologyAndGetMembers( NamedDatabaseId id )
    {
        var coreServerInfo = TestTopology.addressesForCore( 1, false );
        var coreTopology = new DatabaseCoreTopology(
                id.databaseId(), RaftGroupId.from( id.databaseId() ), Map.of( IdFactory.randomServerId(), coreServerInfo ) );
        return coreTopology.members( ( databaseId, serverId ) -> IdFactory.randomRaftMemberId() );
    }
}
