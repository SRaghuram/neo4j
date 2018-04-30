/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.InputStreamReadableChannel;
import org.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;

import static org.junit.Assert.assertEquals;

public class ActorRefMarshalTest
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
    public void shouldMarshalAndUnMarshal() throws IOException, EndOfStreamException
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
        assertEquals( original, result );
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
