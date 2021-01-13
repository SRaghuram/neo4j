/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.Address;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

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
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final NamedDatabaseId namedDatabaseId = databaseIdRepository.getRaw( "known" );
    private final NamedDatabaseId unknownNamedDatabaseId = databaseIdRepository.getRaw( "unknown" );

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
        var clusterViewMessage = newClusterViewMessage( true, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( namedDatabaseId );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress() );

        assertFalse( bootstrapState.canBootstrapRaft( unknownNamedDatabaseId ) );
    }

    @Test
    void shouldNotBootstrapWhenClusterDidNotConverge()
    {
        var me = first( allMembers );
        var clusterViewMessage = newClusterViewMessage( false, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( namedDatabaseId );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress() );

        assertFalse( bootstrapState.canBootstrapRaft( namedDatabaseId ) );
    }

    @Test
    void shouldNotBootstrapWhenNoneThisServerIsNotFirstMember()
    {
        var me = last( allMembers );
        var clusterViewMessage = newClusterViewMessage( true, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( namedDatabaseId );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress() );

        assertFalse( bootstrapState.canBootstrapRaft( namedDatabaseId ) );
    }

    @Test
    void shouldBootstrapWhenThisServerIsTheFirstMember()
    {
        var me = first( allMembers );
        var clusterViewMessage = newClusterViewMessage( true, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( namedDatabaseId );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress() );

        assertTrue( bootstrapState.canBootstrapRaft( namedDatabaseId ) );
    }

    @Test
    void shouldCorrectlyReportIfMemberHasBootstrappedRaftGroup()
    {
        var otherNamedDatabased = databaseIdRepository.getRaw( "otherKnown" );
        var me = last( allMembers );
        var clusterViewMessage = newClusterViewMessage( true, allMembers, List.of() );
        var metadataMessage = newMetadataMessage( namedDatabaseId );

        var myRaftMemberId = IdFactory.randomRaftMemberId();
        var otherRaftMemberId = IdFactory.randomRaftMemberId();

        var previouslyBootstrapped = Map.of(
                RaftGroupId.from( namedDatabaseId.databaseId() ), myRaftMemberId,
                RaftGroupId.from( otherNamedDatabased.databaseId() ), otherRaftMemberId );

        var bootstrapState = newBootstrapState( clusterViewMessage, metadataMessage, me.uniqueAddress(),
                                                previouslyBootstrapped );

        assertFalse( bootstrapState.memberBootstrappedRaft( otherNamedDatabased, myRaftMemberId ) );
        assertFalse( bootstrapState.memberBootstrappedRaft( unknownNamedDatabaseId, myRaftMemberId ) );
        assertTrue( bootstrapState.memberBootstrappedRaft( namedDatabaseId, myRaftMemberId ) );
    }

    private static BootstrapState newBootstrapState( ClusterViewMessage clusterViewMessage, MetadataMessage metadataMessage,
                                                     UniqueAddress uniqueAddress )
    {
        return new BootstrapState( clusterViewMessage, metadataMessage, uniqueAddress, Map.of() );
    }

    private static BootstrapState newBootstrapState( ClusterViewMessage clusterViewMessage,
                                                     MetadataMessage metadataMessage,
                                                     UniqueAddress uniqueAddress,
                                                     Map<RaftGroupId,RaftMemberId> previouslyBootstrapped )
    {
        return new BootstrapState( clusterViewMessage, metadataMessage, uniqueAddress, previouslyBootstrapped );
    }

    private static ClusterViewMessage newClusterViewMessage( boolean converged, List<Member> reachable, List<Member> unreachable )
    {
        var reachableSet = new TreeSet<>( Member.ordering() );
        reachableSet.addAll( reachable );
        var unreachableSet = Set.copyOf( unreachable );
        return new ClusterViewMessage( converged, reachableSet, unreachableSet );
    }

    private MetadataMessage newMetadataMessage( NamedDatabaseId namedDatabaseId )
    {
        var metadata = allMembers.stream().collect( toMap(
                Member::uniqueAddress,
                member -> newCoreInfoForMember( namedDatabaseId ) ) );

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

    private static CoreServerInfoForServerId newCoreInfoForMember( NamedDatabaseId namedDatabaseId )
    {
        var info = newCoreInfo( namedDatabaseId );
        return new CoreServerInfoForServerId( IdFactory.randomServerId(), info );
    }

    private static CoreServerInfo newCoreInfo( NamedDatabaseId namedDatabaseId )
    {
        var raftAddress = new SocketAddress( "neo4j.com", 1 );
        var catchupAddress = new SocketAddress( "neo4j.com", 2 );
        var connectorAddresses = ConnectorAddresses.fromList( List.of() );
        return new CoreServerInfo( raftAddress, catchupAddress, connectorAddresses, Set.of(), Set.of( namedDatabaseId.databaseId() ) );
    }
}
