/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;

public class ReadReplicaRefreshMessageMarshalTest extends BaseMarshalTest<ReadReplicaRefreshMessage>
{
    private static ActorSystem system;

    public ReadReplicaRefreshMessageMarshalTest()
    {
        super( new ReadReplicaRefreshMessage(
                        TestTopology.addressesForReadReplica( 432 ),
                        new MemberId( UUID.randomUUID() ),
                        system.provider().resolveActorRef( String.format( "akka://%s/user/%s", system.name(), ActorRefMarshalTest.Actor.name + "1" ) ),
                        system.provider().resolveActorRef( String.format( "akka://%s/user/%s", system.name(), ActorRefMarshalTest.Actor.name + "2" ) ),
                        defaultDatabaseStates() ), new ReadReplicaRefreshMessageMarshal( (ExtendedActorSystem)system ) );
    }

    @BeforeClass
    public static void setup()
    {
        system = ActorSystem.create();
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name + "1" );
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name + "2" );
    }

    @AfterClass
    public static void teardown()
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
