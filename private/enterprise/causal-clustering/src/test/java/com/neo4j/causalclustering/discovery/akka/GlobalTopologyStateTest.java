/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.ConnectorAddresses.ConnectorUri;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.configuration.ServerGroupName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.AssertableLogProvider;

import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.bolt;
import static com.neo4j.causalclustering.discovery.ConnectorAddresses.Scheme.http;
import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

class GlobalTopologyStateTest
{
    @SuppressWarnings( "unchecked" )
    private final Consumer<DatabaseCoreTopology> listener = mock( Consumer.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final GlobalTopologyState state = new GlobalTopologyState( logProvider, listener );

    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final NamedDatabaseId namedDatabaseId1 = databaseIdRepository.getRaw( "db1" );
    private final DatabaseId databaseId1 = databaseIdRepository.getRaw( "db1" ).databaseId();
    private final NamedDatabaseId namedDatabaseId2 = databaseIdRepository.getRaw( "db2" );
    private final DatabaseId databaseId2 = databaseIdRepository.getRaw( "db2" ).databaseId();

    private final MemberId coreId1 = IdFactory.randomMemberId();
    private final MemberId coreId2 = IdFactory.randomMemberId();
    private final MemberId coreId3 = IdFactory.randomMemberId();
    private final CoreServerInfo coreInfo1 = newCoreInfo( coreId1, Set.of( databaseId1, databaseId2 ) );
    private final CoreServerInfo coreInfo2 = newCoreInfo( coreId2, Set.of( databaseId1 ) );
    private final CoreServerInfo coreInfo3 = newCoreInfo( coreId3, Set.of( databaseId1 ) );

    private final MemberId readReplicaId1 = IdFactory.randomMemberId();
    private final MemberId readReplicaId2 = IdFactory.randomMemberId();
    private final ReadReplicaInfo readReplicaInfo1 = newReadReplicaInfo( readReplicaId1, Set.of( databaseId1, databaseId2 ) );
    private final ReadReplicaInfo readReplicaInfo2 = newReadReplicaInfo( readReplicaId2, Set.of( databaseId2 ) );

    @Test
    void shouldWorkWhenEmpty()
    {
        assertEquals( Map.of(), state.allCoreServers() );
        assertEquals( Map.of(), state.allReadReplicas() );
        assertEquals( DatabaseCoreTopology.empty( databaseId1 ), state.coreTopologyForDatabase( namedDatabaseId1 ) );
        assertEquals( DatabaseReadReplicaTopology.empty( databaseId1 ), state.readReplicaTopologyForDatabase( namedDatabaseId1 ) );
        assertNull( state.retrieveCatchupServerAddress( coreId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId1, coreId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId2, coreId2 ) );
    }

    @Test
    void shouldKeepTrackOfCoreTopologies()
    {
        var coreMembers1 = Map.of( coreId1, coreInfo1, coreId2, coreInfo2 );
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers1 );

        var coreMembers2 = Map.of( coreId2, coreInfo2, coreId3, coreInfo3 );
        var coreTopology2 = new DatabaseCoreTopology( databaseId2, RaftId.from( databaseId2 ), coreMembers2 );

        state.onTopologyUpdate( coreTopology1 );
        state.onTopologyUpdate( coreTopology2 );

        assertEquals( Map.of( coreId1, coreInfo1, coreId2, coreInfo2, coreId3, coreInfo3 ), state.allCoreServers() );
        assertEquals( coreTopology1, state.coreTopologyForDatabase( namedDatabaseId1 ) );
        assertEquals( coreTopology2, state.coreTopologyForDatabase( namedDatabaseId2 ) );
    }

    @Test
    void shouldNotifyCallbackOnCoreTopologyChange()
    {
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ),
                Map.of( coreId1, coreInfo1, coreId2, coreInfo2 ) );
        var coreTopology2 = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId2 ),
                Map.of( coreId1, coreInfo1, coreId2, coreInfo2, coreId3, coreInfo3 ) );

        state.onTopologyUpdate( coreTopology1 );
        verify( listener ).accept( coreTopology1 );

        state.onTopologyUpdate( coreTopology2 );
        verify( listener ).accept( coreTopology2 );
    }

    @Test
    void shouldNotNotifyCallbackIfCoreTopologyDoesNotChange()
    {
        var coreTopology = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), Map.of( coreId1, coreInfo1, coreId2, coreInfo2 ) );

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
        assertEquals( readReplicaTopology1, state.readReplicaTopologyForDatabase( namedDatabaseId1 ) );
        assertEquals( readReplicaTopology2, state.readReplicaTopologyForDatabase( namedDatabaseId2 ) );
    }

    @Test
    void shouldKeepTrackOfLeaderUpdates()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId2, coreInfo2, coreId3, coreInfo3 );

        var coreTopology1 = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers );
        state.onTopologyUpdate( coreTopology1 );
        var coreTopology2 = new DatabaseCoreTopology( databaseId2, RaftId.from( databaseId2 ), coreMembers );
        state.onTopologyUpdate( coreTopology2 );

        var leaderInfos = Map.<DatabaseId,LeaderInfo>of( databaseId1, new LeaderInfo( coreId1, 42 ), databaseId2, new LeaderInfo( coreId3, 4242 ) );

        state.onDbLeaderUpdate( leaderInfos );

        assertEquals( RoleInfo.LEADER, state.role( namedDatabaseId1, coreId1 ) );
        assertEquals( RoleInfo.FOLLOWER, state.role( namedDatabaseId1, coreId2 ) );
        assertEquals( RoleInfo.FOLLOWER, state.role( namedDatabaseId1, coreId3 ) );

        assertEquals( RoleInfo.FOLLOWER, state.role( namedDatabaseId2, coreId1 ) );
        assertEquals( RoleInfo.FOLLOWER, state.role( namedDatabaseId2, coreId2 ) );
        assertEquals( RoleInfo.LEADER, state.role( namedDatabaseId2, coreId3 ) );

        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId1, readReplicaId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId1, readReplicaId2 ) );
        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId2, readReplicaId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId2, readReplicaId2 ) );
    }

    @Test
    void shouldReturnCoreRoleForUnknownDatabase()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId2, coreInfo2, coreId3, coreInfo3 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId2, coreId1 ) );
        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId2, coreId2 ) );
        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId2, coreId3 ) );
    }

    @Test
    void shouldReturnCoreRoleForUnknownMember()
    {
        var coreMembers = Map.of( coreId1, coreInfo1 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId1, coreId2 ) );
        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId1, coreId3 ) );
    }

    @Test
    void shouldReturnCoreRoles()
    {
        var coreMembers1 = Map.of( coreId1, coreInfo1, coreId2, coreInfo2 );
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers1 );

        var coreMembers2 = Map.of( coreId2, coreInfo2, coreId3, coreInfo3 );
        var coreTopology2 = new DatabaseCoreTopology( databaseId2, RaftId.from( databaseId2 ), coreMembers2 );

        state.onTopologyUpdate( coreTopology1 );
        state.onTopologyUpdate( coreTopology2 );

        state.onDbLeaderUpdate( Map.of( databaseId1, new LeaderInfo( coreId1, 42 ), databaseId2, new LeaderInfo( coreId3, 42 ) ) );

        assertEquals( RoleInfo.LEADER, state.role( namedDatabaseId1, coreId1 ) );
        assertEquals( RoleInfo.FOLLOWER, state.role( namedDatabaseId1, coreId2 ) );
        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId1, coreId3 ) );

        assertEquals( RoleInfo.UNKNOWN, state.role( namedDatabaseId2, coreId1 ) );
        assertEquals( RoleInfo.FOLLOWER, state.role( namedDatabaseId2, coreId2 ) );
        assertEquals( RoleInfo.LEADER, state.role( namedDatabaseId2, coreId3 ) );
    }

    @Test
    void shouldReturnAllCores()
    {
        var coreMembers1 = Map.of( coreId1, coreInfo1, coreId3, coreInfo3 );
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers1 );
        state.onTopologyUpdate( coreTopology1 );

        var coreMembers2 = Map.of( coreId2, coreInfo2, coreId3, coreInfo3 );
        var coreTopology2 = new DatabaseCoreTopology( databaseId2, RaftId.from( databaseId2 ), coreMembers2 );
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
        var coreTopology = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( coreTopology, state.coreTopologyForDatabase( namedDatabaseId1 ) );
    }

    @Test
    void shouldReturnCoreTopologyForUnknownDatabase()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId3, coreInfo3 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( DatabaseCoreTopology.empty( databaseId2 ), state.coreTopologyForDatabase( namedDatabaseId2 ) );
    }

    @Test
    void shouldReturnReadReplicaTopologyForKnownDatabase()
    {
        var readReplicas = Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 );
        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId1, readReplicas );
        state.onTopologyUpdate( readReplicaTopology );

        assertEquals( readReplicaTopology, state.readReplicaTopologyForDatabase( namedDatabaseId1 ) );
    }

    @Test
    void shouldReturnReadReplicaTopologyForUnknownDatabase()
    {
        var readReplicas = Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 );
        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId1, readReplicas );
        state.onTopologyUpdate( readReplicaTopology );

        assertEquals( DatabaseReadReplicaTopology.empty( databaseId2 ), state.readReplicaTopologyForDatabase( namedDatabaseId2 ) );
    }

    @Test
    void shouldRetrieveCatchupAddressForCore()
    {
        var coreMembers = Map.of( coreId1, coreInfo1, coreId2, coreInfo2 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers );
        state.onTopologyUpdate( coreTopology );

        assertEquals( coreInfo1.catchupServer(), state.retrieveCatchupServerAddress( coreId1 ) );
        assertEquals( coreInfo2.catchupServer(), state.retrieveCatchupServerAddress( coreId2 ) );
    }

    @Test
    void shouldRetrieveCatchupAddressForReadReplica()
    {
        var readReplicas = Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 );
        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId1, readReplicas );
        state.onTopologyUpdate( readReplicaTopology );

        assertEquals( readReplicaInfo1.catchupServer(), state.retrieveCatchupServerAddress( readReplicaId1 ) );
        assertEquals( readReplicaInfo2.catchupServer(), state.retrieveCatchupServerAddress( readReplicaId2 ) );
    }

    @Test
    void shouldRetrieveNullCatchupAddressForUnknownMember()
    {
        var coreMembers = Map.of( coreId1, coreInfo1 );
        var coreTopology = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), coreMembers );
        state.onTopologyUpdate( coreTopology );

        var readReplicas = Map.of( readReplicaId1, readReplicaInfo1 );
        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId1, readReplicas );
        state.onTopologyUpdate( readReplicaTopology );

        assertNull( state.retrieveCatchupServerAddress( coreId2 ) );
        assertNull( state.retrieveCatchupServerAddress( coreId3 ) );
        assertNull( state.retrieveCatchupServerAddress( readReplicaId2 ) );
    }

    @Test
    void shouldNotStoreEmptyCoreTopologies()
    {
        var coreTopology1 = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), Map.of( coreId1, coreInfo1, coreId2, coreInfo2 ) );
        var coreTopology2 = new DatabaseCoreTopology( databaseId1, RaftId.from( databaseId1 ), emptyMap() );

        state.onTopologyUpdate( coreTopology1 );
        state.onTopologyUpdate( coreTopology2 );

        // changes from both topologies are reported
        verify( listener ).accept( coreTopology1 );
        verify( listener ).accept( coreTopology2 );

        // seconds core topology is not stored because it does not contain any members
        var emptyTopology1 = state.coreTopologyForDatabase( namedDatabaseId1 );
        var emptyTopology2 = state.coreTopologyForDatabase( namedDatabaseId1 );

        assertEquals( emptyTopology1, emptyTopology2 );
        assertNotSame( emptyTopology1, emptyTopology2 );
        assertEquals( DatabaseCoreTopology.empty( databaseId1 ), emptyTopology1 );
        assertEquals( DatabaseCoreTopology.empty( databaseId1 ), emptyTopology2 );
        assertNotEquals( emptyTopology1, coreTopology1 );
        assertNotEquals( emptyTopology1, coreTopology2 );
    }

    @Test
    void shouldNotStoreEmptyReadReplicaTopologies()
    {
        var readReplicaTopology1 = new DatabaseReadReplicaTopology( databaseId1, Map.of( readReplicaId1, readReplicaInfo1, readReplicaId2, readReplicaInfo2 ) );
        var readReplicaTopology2 = new DatabaseReadReplicaTopology( databaseId1, emptyMap() );

        state.onTopologyUpdate( readReplicaTopology1 );
        state.onTopologyUpdate( readReplicaTopology2 );

        // seconds read replica topology is not stored because it does not contain any members
        var emptyTopology1 = state.readReplicaTopologyForDatabase( namedDatabaseId1 );
        var emptyTopology2 = state.readReplicaTopologyForDatabase( namedDatabaseId1 );

        assertEquals( emptyTopology1, emptyTopology2 );
        assertNotSame( emptyTopology1, emptyTopology2 );
        assertNotEquals( emptyTopology1, readReplicaTopology1 );
        assertNotEquals( emptyTopology2, readReplicaTopology1 );
        assertEquals( emptyTopology1, readReplicaTopology2 );
    }

    @Test
    void shouldLogProperlyOnDbLeaderChange()
    {
        // given
        var prefix = "Database leader(s) update:" + lineSeparator() + "  ";
        var leaders = new HashMap<DatabaseId,LeaderInfo>();
        leaders.put( databaseId2, new LeaderInfo( coreId3, 1 ) );
        state.onDbLeaderUpdate( Map.copyOf( leaders ) );

        // when
        logProvider.clear();
        leaders.put( databaseId1, new LeaderInfo( coreId1, 1 ) );
        state.onDbLeaderUpdate( Map.copyOf( leaders ) );
        // then
        assertThat( logProvider ).forClass( GlobalTopologyState.class ).forLevel( INFO ).containsMessages(
                format( "%sDiscovered leader %s in term %d for database %s", prefix, coreId1, 1, databaseId1 ) );

        // when
        logProvider.clear();
        leaders.put( databaseId1, new LeaderInfo( coreId1, 2 ) );
        state.onDbLeaderUpdate( Map.copyOf( leaders ) );
        // then
        assertThat( logProvider ).forClass( GlobalTopologyState.class ).forLevel( INFO ).containsMessages(
                format( "%sDatabase %s leader remains %s but term changed to %d", prefix, databaseId1, coreId1, 2 ) );

        // when
        logProvider.clear();
        leaders.put( databaseId1, new LeaderInfo( coreId2, 3 ) );
        state.onDbLeaderUpdate( Map.copyOf( leaders ) );
        // then
        assertThat( logProvider ).forClass( GlobalTopologyState.class ).forLevel( INFO ).containsMessages(
                format( "%sDatabase %s switch leader from %s to %s in term %d", prefix, databaseId1, coreId1, coreId2, 3 ) );

        // whens
        logProvider.clear();
        leaders.remove( databaseId1 );
        state.onDbLeaderUpdate( Map.copyOf( leaders ) );
        // then
        assertThat( logProvider ).forClass( GlobalTopologyState.class ).forLevel( INFO ).containsMessages(
                format( "%sDatabase %s lost its leader. Previous leader was %s", prefix, databaseId1, coreId2 ) );
    }

    private static CoreServerInfo newCoreInfo( MemberId memberId, Set<DatabaseId> databaseIds )
    {
        var raftAddress = new SocketAddress( "raft-" + memberId.getUuid(), 1 );
        var catchupAddress = new SocketAddress( "catchup-" + memberId.getUuid(), 2 );
        var boltUri = new ConnectorUri( bolt, new SocketAddress( "bolt-" + memberId.getUuid(), 3 ) );
        var httpUri = new ConnectorUri( http, new SocketAddress( "http-" + memberId.getUuid(), 4 ) );
        var connectorUris = ConnectorAddresses.fromList( List.of( boltUri, httpUri ) );
        var groups = ServerGroupName.setOf( "group-1-" + memberId.getUuid(), "group-2-" + memberId.getUuid() );
        var refuseToBeLeader = memberId.getUuid().getLeastSignificantBits() % 2 == 0;
        return new CoreServerInfo( raftAddress, catchupAddress, connectorUris, groups, databaseIds, refuseToBeLeader );
    }

    private static ReadReplicaInfo newReadReplicaInfo( MemberId memberId, Set<DatabaseId> databaseIds )
    {
        var catchupAddress = new SocketAddress( "catchup-" + memberId.getUuid(), 1 );
        var boltUri = new ConnectorUri( bolt, new SocketAddress( "bolt-" + memberId.getUuid(), 2 ) );
        var httpUri = new ConnectorUri( http, new SocketAddress( "http-" + memberId.getUuid(), 3 ) );
        var connectorUris = ConnectorAddresses.fromList( List.of( boltUri, httpUri ) );
        var groups = ServerGroupName.setOf( "group-1-" + memberId.getUuid(), "group-2-" + memberId.getUuid() );
        return new ReadReplicaInfo( connectorUris, catchupAddress, groups, databaseIds );
    }
}
