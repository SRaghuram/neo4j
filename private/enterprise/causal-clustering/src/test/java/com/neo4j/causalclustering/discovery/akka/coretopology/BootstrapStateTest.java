/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.Address;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.refuse_to_be_leader;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterables.first;
import static org.neo4j.internal.helpers.collection.Iterables.last;

class BootstrapStateTest
{
    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final DatabaseId databaseId = databaseIdRepository.get( "known" );
    private final DatabaseId unknownDatabaseId = databaseIdRepository.get( "unknown" );

    private final Member member1 = newMemberMock();
    private final Member member2 = newMemberMock();
    private final Member member3 = newMemberMock();

    private final List<Member> allMembers = Stream.of( member1, member2, member3 )
            .sorted( Member.ordering() )
            .collect( toUnmodifiableList() );

    @Test
    void shouldNotBootstrapUnknownDatabase()
    {
        var me = first( allMembers );
        var iRefuseToBeLeader = false;
        var clusterViewMessage = newClusterViewMessage( true, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( databaseId, me, iRefuseToBeLeader );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress(), iRefuseToBeLeader );

        assertFalse( bootstrapState.canBootstrapRaft( unknownDatabaseId ) );
    }

    @Test
    void shouldNotBootstrapWhenClusterDidNotConverge()
    {
        var me = first( allMembers );
        var iRefuseToBeLeader = false;
        var clusterViewMessage = newClusterViewMessage( false, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( databaseId, me, iRefuseToBeLeader );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress(), iRefuseToBeLeader );

        assertFalse( bootstrapState.canBootstrapRaft( databaseId ) );
    }

    @Test
    void shouldNotBootstrapWhenThisServerRefusesToBeLeader()
    {
        var me = first( allMembers );
        var iRefuseToBeLeader = true;
        var clusterViewMessage = newClusterViewMessage( true, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( databaseId, me, iRefuseToBeLeader );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress(), iRefuseToBeLeader );

        assertFalse( bootstrapState.canBootstrapRaft( databaseId ) );
    }

    @Test
    void shouldNotBootstrapWhenNoneRefuseToBeLeaderButThisServerIsNotFirstMember()
    {
        var me = last( allMembers );
        var iRefuseToBeLeader = false;
        var clusterViewMessage = newClusterViewMessage( true, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( databaseId, me, iRefuseToBeLeader );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress(), iRefuseToBeLeader );

        assertFalse( bootstrapState.canBootstrapRaft( databaseId ) );
    }

    @Test
    void shouldBootstrapWhenNoneRefuseToBeLeaderAndThisServerIsTheFirstMember()
    {
        var me = first( allMembers );
        var iRefuseToBeLeader = false;
        var clusterViewMessage = newClusterViewMessage( true, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( databaseId, me, iRefuseToBeLeader );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress(), iRefuseToBeLeader );

        assertTrue( bootstrapState.canBootstrapRaft( databaseId ) );
    }

    @Test
    void shouldBootstrapWhenThisServerIsTheFirstNonRefuseToBeLeaderMember()
    {
        var me = last( allMembers );
        var iRefuseToBeLeader = false;
        var othersRefuseToBeLeader = true;
        var clusterViewMessage = newClusterViewMessage( true, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( databaseId, me, iRefuseToBeLeader, othersRefuseToBeLeader );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress(), iRefuseToBeLeader );

        assertTrue( bootstrapState.canBootstrapRaft( databaseId ) );
    }

    private static BootstrapState newBootstrapState( ClusterViewMessage clusterViewMessage, MetadataMessage metadataMessage,
            UniqueAddress uniqueAddress, boolean refuseToBeLeader )
    {
        var config = Config.defaults(refuse_to_be_leader, Boolean.toString( refuseToBeLeader ) );

        return new BootstrapState( clusterViewMessage, metadataMessage, uniqueAddress, config );
    }

    private static ClusterViewMessage newClusterViewMessage( boolean converged, List<Member> reachable, List<Member> unreachable )
    {
        var reachableSet = new TreeSet<>( Member.ordering() );
        reachableSet.addAll( reachable );
        var unreachableSet = Set.copyOf( unreachable );
        return new ClusterViewMessage( converged, reachableSet, unreachableSet );
    }

    private MetadataMessage newMetadataMessage( DatabaseId databaseId, Member me, boolean meRefuseToBeLeader )
    {
        var othersRefuseToBeLeader = false;
        return newMetadataMessage( databaseId, me, meRefuseToBeLeader, othersRefuseToBeLeader );
    }

    private MetadataMessage newMetadataMessage( DatabaseId databaseId, Member me, boolean meRefuseToBeLeader, boolean othersRefuseToBeLeader )
    {
        var metadata = allMembers.stream().collect( toMap(
                Member::uniqueAddress,
                member -> newCoreInfoForMember( databaseId, member == me ? meRefuseToBeLeader : othersRefuseToBeLeader ) ) );

        return new MetadataMessage( metadata );
    }

    private static Member newMemberMock()
    {
        var member = mock( Member.class );
        var address = new Address( "protocol", "system" );
        var uniqueAddress = new UniqueAddress( address, ThreadLocalRandom.current().nextLong() );
        when( member.address() ).thenReturn( address );
        when( member.uniqueAddress() ).thenReturn( uniqueAddress );
        when( member.status() ).thenReturn( MemberStatus.up() );
        return member;
    }

    private static CoreServerInfoForMemberId newCoreInfoForMember( DatabaseId databaseId, boolean refuseToBeLeader )
    {
        var info = newCoreInfo( databaseId, refuseToBeLeader );
        return new CoreServerInfoForMemberId( new MemberId( UUID.randomUUID() ), info );
    }

    private static CoreServerInfo newCoreInfo( DatabaseId databaseId, boolean refuseToBeLeader )
    {
        var raftAddress = new SocketAddress( "neo4j.com", 1 );
        var catchupAddress = new SocketAddress( "neo4j.com", 2 );
        var connectorAddresses = new ClientConnectorAddresses( List.of() );
        return new CoreServerInfo( raftAddress, catchupAddress, connectorAddresses, Set.of(), Set.of( databaseId ), refuseToBeLeader );
    }
}
