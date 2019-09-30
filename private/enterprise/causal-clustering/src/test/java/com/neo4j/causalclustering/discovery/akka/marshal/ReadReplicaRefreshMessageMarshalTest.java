/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import akka.testkit.javadsl.TestKit;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRefreshMessage;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.dbms.EnterpriseDatabaseState;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

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

    private static Map<DatabaseId,DatabaseState> defaultDatabaseStates()
    {
        var idRepository = new TestDatabaseIdRepository();
        var defaultDb = idRepository.defaultDatabase();
        var systemDb = SYSTEM_DATABASE_ID;

        return Map.of( systemDb, new EnterpriseDatabaseState( systemDb, STARTED ),
                defaultDb, new EnterpriseDatabaseState( defaultDb, STARTED ) );
    }
}
