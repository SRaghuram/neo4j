/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.ExtendedActorSystem;
import akka.cluster.UniqueAddress;
import akka.testkit.javadsl.TestKit;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForServerId;
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseToMember;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.discovery.akka.directory.ReplicatedLeaderInfo;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRefreshMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRemovalMessage;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.dbms.EnterpriseOperatorState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.kernel.database.DatabaseId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

class BaseAkkaSerializerTest
{
    private static ActorSystem system = ActorSystem.create();

    static
    {
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name + "1" );
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name + "2" );
    }

    @AfterAll
    static void teardown()
    {
        TestKit.shutdownActorSystem( system );
        system = null;
    }

    static Stream<Object[]> data()
    {
        String actorPath = String.format( "akka://%s/user/%s", system.name(), ActorRefMarshalTest.Actor.name );
        var randomDbId = randomDatabaseId();
        var randomRaftId = RaftId.from( randomDbId );
        return Stream.of(
                new Object[]{new LeaderInfo( IdFactory.randomMemberId(), 37L ), new LeaderInfoSerializer()},
                new Object[]{IdFactory.randomRaftId(), new RaftIdSerializer()},
                new Object[]{new UniqueAddress( new Address( "protocol", "system" ), 398L ), new UniqueAddressSerializer()},
                new Object[]{new UniqueAddress( new Address( "protocol", "system", "host", 79 ), 398L ),
                             new UniqueAddressSerializer()},
                new Object[]{new CoreServerInfoForServerId( IdFactory.randomServerId(), TestTopology.addressesForCore( 1, false ) ),
                             new CoreServerInfoForServerIdSerializer()},
                new Object[]{new ReadReplicaRefreshMessage(
                        TestTopology.addressesForReadReplica( 432 ),
                        IdFactory.randomServerId(),
                        system.provider().resolveActorRef( actorPath + 1 ),
                        system.provider().resolveActorRef( actorPath + 2 ),
                        Collections.emptyMap() ),
                             new ReadReplicaRefreshMessageSerializer( (ExtendedActorSystem) system )},
                new Object[]{IdFactory.randomMemberId(),
                             new MemberIdSerializer()},
                new Object[]{TestTopology.addressesForReadReplica( 74839 ),
                             new ReadReplicaInfoSerializer()},
                new Object[]{new DatabaseCoreTopology( randomDbId, randomRaftId,
                        CoreTopologyMarshalTest.coreServerInfos( 3 ) ), new CoreTopologySerializer()},
                new Object[]{new ReadReplicaRemovalMessage( system.provider().resolveActorRef( actorPath + 2 ) ),
                             new ReadReplicaRemovalMessageSerializer( (ExtendedActorSystem) system )},
                new Object[]{ReadReplicaTopologyMarshalTest.generate(), new ReadReplicaTopologySerializer()},
                new Object[]{LeaderInfoDirectoryMessageMarshalTest.generate(), new DatabaseLeaderInfoMessageSerializer()},
                new Object[]{new ReplicatedLeaderInfo( new LeaderInfo( IdFactory.randomMemberId(), 14L ) ), new ReplicatedLeaderInfoSerializer()},
                new Object[]{randomDatabaseId(), new DatabaseIdWithoutNameSerializer()},
                new Object[]{randomDatabaseToMember(), new DatabaseToMemberSerializer()},
                new Object[]{randomDatabaseState( randomDatabaseId() ), new DiscoveryDatabaseStateSerializer()},
                new Object[]{randomReplicatedState(), new ReplicatedDatabaseStateSerializer()}
        );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldSerializeAndDeserialize( Object original, BaseAkkaSerializer<?> serializer )
    {
        // when
        byte[] binary = serializer.toBinary( original );
        Object result = serializer.fromBinaryJava( binary, original.getClass() );

        // then
        assertThat( original ).isNotSameAs( result );
        assertEquals( original, result );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldHaveAppropriateSizeHint( Object original, BaseAkkaSerializer<?> serializer )
    {
        // when
        byte[] binary = serializer.toBinary( original );

        // then
        assertThat( binary.length ).isLessThanOrEqualTo( serializer.sizeHint() );
    }

    private static DatabaseToMember randomDatabaseToMember()
    {
        return new DatabaseToMember( randomDatabaseId(), IdFactory.randomMemberId() );
    }

    private static DiscoveryDatabaseState randomDatabaseState( DatabaseId databaseId )
    {
        int lim = EnterpriseOperatorState.values().length;
        int randomIdx = ThreadLocalRandom.current().nextInt( lim );
        var randomState = EnterpriseOperatorState.values()[randomIdx];
        return new DiscoveryDatabaseState( databaseId, randomState );
    }

    private static ReplicatedDatabaseState randomReplicatedState()
    {
        var isCore = ThreadLocalRandom.current().nextBoolean();
        var id = randomDatabaseId();
        var randomMemberStates = Stream.generate( () -> Map.entry( IdFactory.randomMemberId(), randomDatabaseState( id ) ) )
                .limit( 5 )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
        if ( isCore )
        {
            return ReplicatedDatabaseState.ofCores( id, randomMemberStates );
        }
        return ReplicatedDatabaseState.ofReadReplicas( id, randomMemberStates );
    }
}
