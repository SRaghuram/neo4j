/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorRef;
import akka.actor.ExtendedActorSystem;
import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRefreshMessage;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

import java.io.IOException;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class ReadReplicaRefreshMessageMarshal extends SafeChannelMarshal<ReadReplicaRefreshMessage>
{
    private final ChannelMarshal<ReadReplicaInfo> readReplicaInfoMarshal = new ReadReplicaInfoMarshal();
    private final ChannelMarshal<MemberId> memberIdMarshal = new MemberId.Marshal();
    private final ChannelMarshal<ActorRef> actorRefMarshal;

    public ReadReplicaRefreshMessageMarshal( ExtendedActorSystem system )
    {
        this.actorRefMarshal = new ActorRefMarshal( system );
    }

    @Override
    protected ReadReplicaRefreshMessage unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        ReadReplicaInfo rrInfo = readReplicaInfoMarshal.unmarshal( channel );
        MemberId memberId = memberIdMarshal.unmarshal( channel );
        ActorRef clusterClient = actorRefMarshal.unmarshal( channel );
        ActorRef topologyClient = actorRefMarshal.unmarshal( channel );

        return new ReadReplicaRefreshMessage( rrInfo, memberId, clusterClient, topologyClient );
    }

    @Override
    public void marshal( ReadReplicaRefreshMessage readReplicaRefreshMessage, WritableChannel channel ) throws IOException
    {
        readReplicaInfoMarshal.marshal( readReplicaRefreshMessage.readReplicaInfo(), channel );
        memberIdMarshal.marshal( readReplicaRefreshMessage.memberId(), channel );
        actorRefMarshal.marshal( readReplicaRefreshMessage.clusterClient(), channel );
        actorRefMarshal.marshal( readReplicaRefreshMessage.topologyClientActorRef(), channel );
    }
}
