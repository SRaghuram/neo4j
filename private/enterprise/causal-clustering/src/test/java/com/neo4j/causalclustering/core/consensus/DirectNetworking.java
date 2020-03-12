/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.MemberId;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DirectNetworking
{
    private final Map<MemberId,com.neo4j.causalclustering.messaging.Inbound.MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>>> handlers =
            new HashMap<>();
    private final Map<MemberId,Queue<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>>> messageQueues = new HashMap<>();
    private final Set<MemberId> disconnectedMembers = Collections.newSetFromMap( new ConcurrentHashMap<>() );

    public void processMessages()
    {
        while ( messagesToBeProcessed() )
        {
            for ( Map.Entry<MemberId,Queue<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>>> entry : messageQueues.entrySet() )
            {
                MemberId id = entry.getKey();
                Queue<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> queue = entry.getValue();
                if ( !queue.isEmpty() )
                {
                    var message = queue.remove();
                    handlers.get( id ).handle( message );
                }
            }
        }
    }

    private boolean messagesToBeProcessed()
    {
        for ( Queue<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> queue : messageQueues.values() )
        {
            if ( !queue.isEmpty() )
            {
                return true;
            }
        }
        return false;
    }

    public void disconnect( MemberId id )
    {
        disconnectedMembers.add( id );
    }

    public void reconnect( MemberId id )
    {
        disconnectedMembers.remove( id );
    }

    public class Outbound implements com.neo4j.causalclustering.messaging.Outbound<MemberId,RaftMessages.RaftMessage>
    {
        private final MemberId me;

        public Outbound( MemberId me )
        {
            this.me = me;
        }

        @Override
        public synchronized void send( MemberId to, final RaftMessages.RaftMessage message, boolean block )
        {
            if ( canDeliver( to ) )
            {
                messageQueues.get( to ).add( RaftMessages.ReceivedInstantRaftIdAwareMessage.of( Instant.now(), null, message ) );
            }
        }

        private boolean canDeliver( MemberId to )
        {
            return messageQueues.containsKey( to ) &&
                    !disconnectedMembers.contains( to ) &&
                    !disconnectedMembers.contains( me );
        }
    }

    public class Inbound implements com.neo4j.causalclustering.messaging.Inbound<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>>
    {
        private final MemberId id;

        public Inbound( MemberId id )
        {
            this.id = id;
        }

        @Override
        public void registerHandler( MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>> handler )
        {
            handlers.put( id, handler );
            messageQueues.put( id, new LinkedList<>() );
        }
    }
}
