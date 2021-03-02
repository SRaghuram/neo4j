/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.ExtendedActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.testkit.javadsl.TestKit;
import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import scala.concurrent.duration.FiniteDuration;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.marshal.ChannelMarshal;

class ActorRefMarshalTest implements BaseMarshalTest<ActorRef>
{
    private static ActorSystem system;

    @Override
    public Collection<ActorRef> originals()
    {
        return List.of( system.provider().resolveActorRef( String.format( "akka://%s/user/%s", system.name(), Actor.name ) ) );
    }

    @Override
    public ChannelMarshal<ActorRef> marshal()
    {
        return new ActorRefMarshal( (ExtendedActorSystem) system );
    }

    @Override
    public boolean unMarshalCreatesNewRefs()
    {
        return false;
    }

    @Override
    public boolean singletonMarshal()
    {
        return false;
    }

    static class Actor extends AbstractActor
    {
        static final String name = "foo";

        static Props props()
        {
            return Props.create( Actor.class );
        }

        @Override
        public Receive createReceive()
        {
            return ReceiveBuilder.create().build();
        }
    }

    @BeforeAll
    static void setup()
    {
        system = ActorSystem.create();
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name );
    }

    @AfterAll
    static void teardown()
    {
        TestKit.shutdownActorSystem(system,  new FiniteDuration( 20, TimeUnit.SECONDS ), true );
        system = null;
    }
}
