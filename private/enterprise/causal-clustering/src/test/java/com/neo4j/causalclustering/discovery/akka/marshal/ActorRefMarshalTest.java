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
import com.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.FiniteDuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.marshal.EndOfStreamException;

class ActorRefMarshalTest
{
    private static ActorSystem system;

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

    @Test
    void shouldMarshalAndUnMarshal() throws IOException, EndOfStreamException
    {
        ActorRefMarshal marshal = new ActorRefMarshal( (ExtendedActorSystem) system );
        ActorRef original = system.provider().resolveActorRef( String.format( "akka://%s/user/%s", system.name(), Actor.name ) );
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // when
        OutputStreamWritableChannel writableChannel = new OutputStreamWritableChannel( outputStream );
        marshal.marshal( original, writableChannel );

        InputStreamReadableChannel readableChannel = new InputStreamReadableChannel( new ByteArrayInputStream( outputStream.toByteArray() ) );
        ActorRef result = marshal.unmarshal( readableChannel );

        // then
        Assertions.assertEquals( original, result );
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
