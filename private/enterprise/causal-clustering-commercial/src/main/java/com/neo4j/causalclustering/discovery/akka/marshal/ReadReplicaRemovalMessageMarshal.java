/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorRef;
import akka.actor.ExtendedActorSystem;

import java.io.IOException;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.discovery.akka.ReadReplicaRemovalMessage;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReadReplicaRemovalMessageMarshal extends SafeChannelMarshal<ReadReplicaRemovalMessage>
{
    private final ChannelMarshal<ActorRef> actorRefMarshal;

    public ReadReplicaRemovalMessageMarshal( ExtendedActorSystem system )
    {
        this.actorRefMarshal = new ActorRefMarshal( system );
    }

    @Override
    protected ReadReplicaRemovalMessage unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        return new ReadReplicaRemovalMessage( actorRefMarshal.unmarshal( channel ) );
    }

    @Override
    public void marshal( ReadReplicaRemovalMessage readReplicaRemovalMessage, WritableChannel channel ) throws IOException
    {
        actorRefMarshal.marshal( readReplicaRemovalMessage.clusterClient(), channel );
    }
}
