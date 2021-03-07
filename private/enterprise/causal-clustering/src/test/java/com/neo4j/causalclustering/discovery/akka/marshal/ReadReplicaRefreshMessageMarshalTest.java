/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import akka.testkit.javadsl.TestKit;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRefreshMessage;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;

public class ReadReplicaRefreshMessageMarshalTest implements BaseMarshalTest<ReadReplicaRefreshMessage>
{
    private static ActorSystem system;

    @Override
    public Collection<ReadReplicaRefreshMessage> originals()
    {
        return List.of( new ReadReplicaRefreshMessage(
                TestTopology.addressesForReadReplica( 432 ),
                IdFactory.randomServerId(),
                system.provider().resolveActorRef( String.format( "akka://%s/user/%s", system.name(), ActorRefMarshalTest.Actor.name + "1" ) ),
                system.provider().resolveActorRef( String.format( "akka://%s/user/%s", system.name(), ActorRefMarshalTest.Actor.name + "2" ) ),
                defaultDatabaseStates() ) );
    }

    @Override
    public ChannelMarshal<ReadReplicaRefreshMessage> marshal()
    {
        return new ReadReplicaRefreshMessageMarshal( (ExtendedActorSystem) system );
    }

    @Override
    public boolean singletonMarshal()
    {
        return false;
    }

    @BeforeAll
    void setup()
    {
        system = ActorSystem.create();
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name + "1" );
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name + "2" );
    }

    @AfterAll
    void teardown()
    {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    private static Map<DatabaseId,DiscoveryDatabaseState> defaultDatabaseStates()
    {
        var idRepository = new TestDatabaseIdRepository();
        var defaultDb = idRepository.defaultDatabase().databaseId();
        var systemDb = DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID.databaseId();

        return Map.of( systemDb, new DiscoveryDatabaseState( systemDb, STARTED ),
                defaultDb, new DiscoveryDatabaseState( defaultDb, STARTED ) );
    }
}
