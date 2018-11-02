/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.heartbeat;

import java.net.URI;
import java.net.URISyntaxException;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

/**
 * When a message is sent out, reset the timeout for sending heartbeat to the HEADER_TO host, since we only have to send i_am_alive if
 * nothing else is going on.
 */
public class HeartbeatRefreshProcessor implements MessageProcessor
{
    private final MessageHolder outgoing;
    private final ClusterContext clusterContext;

    public HeartbeatRefreshProcessor( MessageHolder outgoing, ClusterContext clusterContext )
    {
        this.outgoing = outgoing;
        this.clusterContext = clusterContext;
    }

    @Override
    public boolean process( Message<? extends MessageType> message )
    {
        if ( !message.isInternal() &&
                !message.getMessageType().equals( HeartbeatMessage.i_am_alive ) )
        {
            try
            {
                String to = message.getHeader( Message.HEADER_TO );

                InstanceId serverId = clusterContext.getConfiguration().getIdForUri( new URI( to ) );

                if ( !clusterContext.isMe( serverId ) )
                {
                    outgoing.offer( Message.internal( HeartbeatMessage.reset_send_heartbeat,
                            serverId ) );
                }
            }
            catch ( URISyntaxException e )
            {
                e.printStackTrace();
            }
        }
        return true;
    }
}
