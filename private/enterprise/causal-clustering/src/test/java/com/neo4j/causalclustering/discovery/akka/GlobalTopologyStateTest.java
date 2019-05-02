/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses.ConnectorUri;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.http;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class GlobalTopologyStateTest
{
    @SuppressWarnings( "unchecked" )
    private final Consumer<DatabaseCoreTopology> listener = mock( Consumer.class );
    private final GlobalTopologyState state = new GlobalTopologyState( NullLogProvider.getInstance(), listener );

    private final DatabaseId databaseId1 = new DatabaseId( "db1" );
    private final DatabaseId databaseId2 = new DatabaseId( "db2" );

    private final MemberId coreId1 = new MemberId( UUID.randomUUID() );
    private final MemberId coreId2 = new MemberId( UUID.randomUUID() );
    private final MemberId coreId3 = new MemberId( UUID.randomUUID() );
    private final CoreServerInfo coreInfo1 = newCoreInfo( coreId1, Set.of( databaseId1, databaseId2 ) );
    private final CoreServerInfo coreInfo2 = newCoreInfo( coreId2, Set.of( databaseId1 ) );
    private final CoreServerInfo coreInfo3 = newCoreInfo( coreId3, Set.of( databaseId1 ) );

    private final MemberId readReplicaId1 = new MemberId( UUID.randomUUID() );
    private final MemberId readReplicaId2 = new MemberId( UUID.randomUUID() );
    private final ReadReplicaInfo readReplicaInfo1 = newReadReplicaInfo( readReplicaId1, Set.of( databaseId1, databaseId2 ) );
    private final ReadReplicaInfo readReplicaInfo2 = newReadReplicaInfo( readReplicaId2, Set.of( databaseId2 ) );

    private final ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    @Test
    void shouldWorkWhenEmpty()
    {
        assertEquals( Map.of(), state.allCoreServers() );
        assertEquals( Map.of(), state.allReadReplicas() );
        assertEquals( DatabaseCoreTopology.EMPTY, state.coreTopologyForDatabase( databaseId1 ) );
        assertEquals( DatabaseReadReplicaTopology.EMPTY, state.readReplicaTopologyForDatabase( databaseId1 ) );
        assertNull( state.retrieveCatchupServerAddress( coreId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId1, coreId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId2, coreId2 ) );
    }

    @Test
    void shouldKeepTrackOfCoreTopologies()
    {
        var coreMembers1 = Map.of( coreId1, coreInfo1, coreId2, coreInfo2 );
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers1 );

        var coreMembers2 = Map.of( coreId2, coreInfo2, coreId3, coreInfo3 );
        var coreTopology2 = new DatabaseCoreTopology( databaseId2, clusterId, true, coreMembers2 );

        state.onTopologyUpdate( coreTopology1 );
        state.onTopologyUpdate( coreTopology2 );

        assertEquals( Map.of( coreId1, coreInfo1, coreId2, coreInfo2, coreId3, coreInfo3 ), state.allCoreServers() );
        assertEquals( coreTopology1, state.coreTopologyForDatabase( databaseId1 ) );
        assertEquals( coreTopology2, state.coreTopologyForDatabase( databaseId2 ) );
    }

    @Test
    void shouldNotifyCallbackOnCoreTopologyChange()
    {
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, clusterId, false, Map.of( coreId1, coreInfo1, coreId2, coreInfo2 ) );
        var coreTopology2 = new DatabaseCoreTopology( databaseId1, clusterId, false, Map.of( coreId1, coreInfo1, coreId2, coreInfo2, coreId3, coreInfo3 ) );

        state.onTopologyUpdate( coreTopology1 );
        verify( listener ).accept( coreTopology1 );

        state.onTopologyUpdate( coreTopology2 );
        verify( listener ).accept( coreTopology2 );
    }

    @Test
    void shouldNotNotifyCallbackIfCoreTopologyDoesNotChange()
    {
        var coreTopology = new DatabaseCoreTopology( databaseId1, clusterId, false, Map.of( coreId1, coreInfo1, coreId2, coreInfo2 ) );

        state.onTopologyUpdate( coreTopology );
        state.onTopologyUpdate( coreTopology );
        state.onTopologyUpdate( coreTopology );

        verify( listener ).accept( coreTopology );
    }

    @Test
    void shouldKeepTrackOfReadReplicaTopologies()
    {
        var readReplicas1 = Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 );
        var readReplicaTopology1 = new DatabaseReadReplicaTopology( databaseId1, readReplicas1 );

        var readReplicas2 = Map.of( readReplicaId2, readReplicaInfo2 );
        var readReplicaTopology2 = new DatabaseReadReplicaTopology( databaseId2, readReplicas2 );

        state.onTopologyUpdate( readReplicaTopology1 );
        state.onTopologyUpdate( readReplicaTopology2 );

        assertEquals( Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 ), state.allReadReplicas() );
        assertEquals( readReplicaTopology1, state.readReplicaTopologyForDatabase( databaseId1 ) );
        assertEquals( readReplicaTopology2, state.readReplicaTopologyForDatabase( databaseId2 ) );
    }

    @Test
    void shouldKeepTrackOfLeaderUpdates()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId2, coreInfo2, coreId3, coreInfo3 );

        var coreTopology1 = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers );
        state.onTopologyUpdate( coreTopology1 );
        var coreTopology2 = new DatabaseCoreTopology( databaseId2, clusterId, false, coreMembers );
        state.onTopologyUpdate( coreTopology2 );

        var leaderInfos = Map.of( databaseId1, new LeaderInfo( coreId1, 42 ), databaseId2, new LeaderInfo( coreId3, 4242 ) );

        state.onDbLeaderUpdate( leaderInfos );

        assertEquals( RoleInfo.LEADER, state.coreRole( databaseId1, coreId1 ) );
        assertEquals( RoleInfo.FOLLOWER, state.coreRole( databaseId1, coreId2 ) );
        assertEquals( RoleInfo.FOLLOWER, state.coreRole( databaseId1, coreId3 ) );

        assertEquals( RoleInfo.FOLLOWER, state.coreRole( databaseId2, coreId1 ) );
        assertEquals( RoleInfo.FOLLOWER, state.coreRole( databaseId2, coreId2 ) );
        assertEquals( RoleInfo.LEADER, state.coreRole( databaseId2, coreId3 ) );

        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId1, readReplicaId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId1, readReplicaId2 ) );
        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId2, readReplicaId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId2, readReplicaId2 ) );
    }

    @Test
    void shouldReturnCoreRoleForUnknownDatabase()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId2, coreInfo2, coreId3, coreInfo3 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId2, coreId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId2, coreId2 ) );
        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId2, coreId3 ) );
    }

    @Test
    void shouldReturnCoreRoleForUnknownMember()
    {
        var coreMembers = Map.of( coreId1, coreInfo1 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId1, coreId2 ) );
        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId1, coreId3 ) );
    }

    @Test
    void shouldReturnCoreRoles()
    {
        var coreMembers1 = Map.of( coreId1, coreInfo1, coreId2, coreInfo2 );
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers1 );

        var coreMembers2 = Map.of( coreId2, coreInfo2, coreId3, coreInfo3 );
        var coreTopology2 = new DatabaseCoreTopology( databaseId2, clusterId, false, coreMembers2 );

        state.onTopologyUpdate( coreTopology1 );
        state.onTopologyUpdate( coreTopology2 );

        state.onDbLeaderUpdate( Map.of( databaseId1, new LeaderInfo( coreId1, 42 ), databaseId2, new LeaderInfo( coreId3, 42 ) ) );

        assertEquals( RoleInfo.LEADER, state.coreRole( databaseId1, coreId1 ) );
        assertEquals( RoleInfo.FOLLOWER, state.coreRole( databaseId1, coreId2 ) );
        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId1, coreId3 ) );

        assertEquals( RoleInfo.UNKNOWN, state.coreRole( databaseId2, coreId1 ) );
        assertEquals( RoleInfo.FOLLOWER, state.coreRole( databaseId2, coreId2 ) );
        assertEquals( RoleInfo.LEADER, state.coreRole( databaseId2, coreId3 ) );
    }

    @Test
    void shouldReturnAllCores()
    {
        var coreMembers1 = Map.of( coreId1, coreInfo1, coreId3, coreInfo3 );
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers1 );
        state.onTopologyUpdate( coreTopology1 );

        var coreMembers2 = Map.of( coreId2, coreInfo2, coreId3, coreInfo3 );
        var coreTopology2 = new DatabaseCoreTopology( databaseId2, clusterId, false, coreMembers2 );
        state.onTopologyUpdate( coreTopology2 );

        assertEquals( Map.of( coreId1, coreInfo1, coreId2, coreInfo2, coreId3, coreInfo3 ), state.allCoreServers() );
    }

    @Test
    void shouldReturnAllReadReplicas()
    {
        var readReplicas1 = Map.of( readReplicaId1, readReplicaInfo1 );
        var readReplicaTopology1 = new DatabaseReadReplicaTopology( databaseId1, readReplicas1 );
        state.onTopologyUpdate( readReplicaTopology1 );

        var readReplicas2 = Map.of( readReplicaId2, readReplicaInfo2 );
        var readReplicaTopology2 = new DatabaseReadReplicaTopology( databaseId2, readReplicas2 );
        state.onTopologyUpdate( readReplicaTopology2 );

        assertEquals( Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 ), state.allReadReplicas() );
    }

    @Test
    void shouldReturnCoreTopologyForKnownDatabase()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId3, coreInfo3 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( coreTopology, state.coreTopologyForDatabase( databaseId1 ) );
    }

    @Test
    void shouldReturnCoreTopologyForUnknownDatabase()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId3, coreInfo3 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( DatabaseCoreTopology.EMPTY, state.coreTopologyForDatabase( databaseId2 ) );
    }

    @Test
    void shouldReturnReadReplicaTopologyForKnownDatabase()
    {
        var readReplicas = Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 );
        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId1, readReplicas );
        state.onTopologyUpdate( readReplicaTopology );

        assertEquals( readReplicaTopology, state.readReplicaTopologyForDatabase( databaseId1 ) );
    }

    @Test
    void shouldReturnReadReplicaTopologyForUnknownDatabase()
    {
        var readReplicas = Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 );
        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId1, readReplicas );
        state.onTopologyUpdate( readReplicaTopology );

        assertEquals( DatabaseReadReplicaTopology.EMPTY, state.readReplicaTopologyForDatabase( databaseId2 ) );
    }

    @Test
    void shouldRetrieveCatchupAddressForCore()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId2, coreInfo2 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( coreInfo1.getCatchupServer(), state.retrieveCatchupServerAddress( coreId1 ) );
        assertEquals( coreInfo2.getCatchupServer(), state.retrieveCatchupServerAddress( coreId2 ) );
    }

    @Test
    void shouldRetrieveCatchupAddressForReadReplica()
    {
        var readReplicas = Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 );
        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId1, readReplicas );
        state.onTopologyUpdate( readReplicaTopology );

        assertEquals( readReplicaInfo1.getCatchupServer(), state.retrieveCatchupServerAddress( readReplicaId1 ) );
        assertEquals( readReplicaInfo2.getCatchupServer(), state.retrieveCatchupServerAddress( readReplicaId2 ) );
    }

    @Test
    void shouldRetrieveNullCatchupAddressForUnknownMember()
    {
        var coreMembers = Map.of( coreId1, coreInfo1 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers );
        state.onTopologyUpdate( coreTopology );

        var readReplicas = Map.of( readReplicaId1, readReplicaInfo1 );
        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId1, readReplicas );
        state.onTopologyUpdate( readReplicaTopology );

        assertNull( state.retrieveCatchupServerAddress( coreId2 ) );
        assertNull( state.retrieveCatchupServerAddress( coreId3 ) );
        assertNull( state.retrieveCatchupServerAddress( readReplicaId2 ) );
    }

    @Test
    void shouldIgnoreCoreTopologyThatDiffersOnlyInCanBeBootstrapped()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId2, coreInfo2 );
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, clusterId, false, coreMembers );
        var coreTopology2 = new DatabaseCoreTopology( databaseId1, clusterId, true, coreMembers );

        state.onTopologyUpdate( coreTopology1 );
        state.onTopologyUpdate( coreTopology2 );

        verify( listener ).accept( coreTopology1 );
        verify( listener, never() ).accept( coreTopology2 );
    }

    private static CoreServerInfo newCoreInfo( MemberId memberId, Set<DatabaseId> databaseIds )
    {
        var raftAddress = new AdvertisedSocketAddress( "raft-" + memberId.getUuid(), 1 );
        var catchupAddress = new AdvertisedSocketAddress( "catchup-" + memberId.getUuid(), 2 );
        var boltUri = new ConnectorUri( bolt, new AdvertisedSocketAddress( "bolt-" + memberId.getUuid(), 3 ) );
        var httpUri = new ConnectorUri( http, new AdvertisedSocketAddress( "http-" + memberId.getUuid(), 4 ) );
        var connectorUris = new ClientConnectorAddresses( List.of( boltUri, httpUri ) );
        var groups = Set.of( "group-1-" + memberId.getUuid(), "group-2-" + memberId.getUuid() );
        var refuseToBeLeader = memberId.getUuid().getLeastSignificantBits() % 2 == 0;
        return new CoreServerInfo( raftAddress, catchupAddress, connectorUris, groups, databaseIds, refuseToBeLeader );
    }

    private static ReadReplicaInfo newReadReplicaInfo( MemberId memberId, Set<DatabaseId> databaseIds )
    {
        var catchupAddress = new AdvertisedSocketAddress( "catchup-" + memberId.getUuid(), 1 );
        var boltUri = new ConnectorUri( bolt, new AdvertisedSocketAddress( "bolt-" + memberId.getUuid(), 2 ) );
        var httpUri = new ConnectorUri( http, new AdvertisedSocketAddress( "http-" + memberId.getUuid(), 3 ) );
        var connectorUris = new ClientConnectorAddresses( List.of( boltUri, httpUri ) );
        var groups = Set.of( "group-1-" + memberId.getUuid(), "group-2-" + memberId.getUuid() );
        return new ReadReplicaInfo( connectorUris, catchupAddress, groups, databaseIds );
    }
}
