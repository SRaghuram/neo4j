/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import akka.testkit.javadsl.TestKit;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRemovalMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.Collection;

import org.neo4j.io.marshal.ChannelMarshal;

import static java.util.Collections.singletonList;

public class ReadReplicaRemovalMarshalTest extends BaseMarshalTest<ReadReplicaRemovalMessage>
{
    private static ActorSystem system;

    @Override
    Collection<ReadReplicaRemovalMessage> originals()
    {
        return singletonList( new ReadReplicaRemovalMessage(
                system.provider().resolveActorRef( String.format( "akka://%s/user/%s", system.name(), ActorRefMarshalTest.Actor.name ) ) ) );
    }

    @Override
    ChannelMarshal<ReadReplicaRemovalMessage> marshal()
    {
        return new ReadReplicaRemovalMessageMarshal( (ExtendedActorSystem) system );
    }

    @BeforeAll
    void setup()
    {
        system = ActorSystem.create();
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name );
    }

    @AfterAll
    void teardown()
    {
        TestKit.shutdownActorSystem(system);
        system = null;
    }
}
