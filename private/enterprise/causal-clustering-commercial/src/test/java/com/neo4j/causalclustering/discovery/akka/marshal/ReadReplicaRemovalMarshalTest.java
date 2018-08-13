/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRemovalMessage;

public class ReadReplicaRemovalMarshalTest extends BaseMarshalTest<ReadReplicaRemovalMessage>
{
    private static ActorSystem system;

    public ReadReplicaRemovalMarshalTest()
    {
        super( new ReadReplicaRemovalMessage(
                        system.provider().resolveActorRef( String.format( "akka://%s/user/%s", system.name(), ActorRefMarshalTest.Actor.name ) ) ),
                new ReadReplicaRemovalMessageMarshal( (ExtendedActorSystem) system ) );
    }

    @BeforeClass
    public static void setup()
    {
        system = ActorSystem.create();
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name );
    }

    @AfterClass
    public static void teardown()
    {
        TestKit.shutdownActorSystem(system);
        system = null;
    }
}
