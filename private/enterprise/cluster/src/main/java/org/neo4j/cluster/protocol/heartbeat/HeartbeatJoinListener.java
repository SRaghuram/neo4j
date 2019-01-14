/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.heartbeat;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterListener;

/**
 * When an instance joins a cluster, setup a heartbeat for it
 */
public class HeartbeatJoinListener extends ClusterListener.Adapter
{
    private final MessageHolder outgoing;

    public HeartbeatJoinListener( MessageHolder outgoing )
    {
        this.outgoing = outgoing;
    }

    @Override
    public void joinedCluster( InstanceId member, URI atUri )
    {
        outgoing.offer( Message.internal( HeartbeatMessage.reset_send_heartbeat, member ) );
    }
}
