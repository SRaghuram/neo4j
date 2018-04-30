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
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.akka.ReadReplicaInfoMessage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReadReplicaInfoMessageMarshal extends SafeChannelMarshal<ReadReplicaInfoMessage>
{
    private final ChannelMarshal<ReadReplicaInfo> readReplicaInfoMarshal = new ReadReplicaInfoMarshal();
    private final ChannelMarshal<MemberId> memberIdMarshal = new MemberId.Marshal();
    private final ChannelMarshal<ActorRef> actorRefMarshal;

    public ReadReplicaInfoMessageMarshal( ExtendedActorSystem system )
    {
        this.actorRefMarshal = new ActorRefMarshal( system );
    }

    @Override
    protected ReadReplicaInfoMessage unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        ReadReplicaInfo rrInfo = readReplicaInfoMarshal.unmarshal( channel );
        MemberId memberId = memberIdMarshal.unmarshal( channel );
        ActorRef clusterClient = actorRefMarshal.unmarshal( channel );
        ActorRef topologyClient = actorRefMarshal.unmarshal( channel );

        return new ReadReplicaInfoMessage( rrInfo, memberId, clusterClient, topologyClient );
    }

    @Override
    public void marshal( ReadReplicaInfoMessage readReplicaInfoMessage, WritableChannel channel ) throws IOException
    {
        readReplicaInfoMarshal.marshal( readReplicaInfoMessage.readReplicaInfo(), channel );
        memberIdMarshal.marshal( readReplicaInfoMessage.memberId(), channel );
        actorRefMarshal.marshal( readReplicaInfoMessage.clusterClient(), channel );
        actorRefMarshal.marshal( readReplicaInfoMessage.topologyClientActorRef(), channel );
    }
}
