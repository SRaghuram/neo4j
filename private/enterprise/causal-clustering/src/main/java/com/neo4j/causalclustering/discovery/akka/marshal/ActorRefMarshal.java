/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorRef;
import akka.actor.ExtendedActorSystem;
import akka.serialization.Serialization;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.SafeChannelMarshal;

public class ActorRefMarshal extends SafeChannelMarshal<ActorRef>
{
    private final ExtendedActorSystem actorSystem;

    public ActorRefMarshal( ExtendedActorSystem actorSystem )
    {
        this.actorSystem = actorSystem;
    }

    @Override
    protected ActorRef unmarshal0( ReadableChannel channel ) throws IOException
    {
        String actorRefPath = StringMarshal.unmarshal( channel );
        return actorSystem.provider().resolveActorRef( actorRefPath );
    }

    @Override
    public void marshal( ActorRef actorRef, WritableChannel channel ) throws IOException
    {
        String actorPath = Serialization.serializedActorPath( actorRef );
        StringMarshal.marshal( channel, actorPath );
    }
}
