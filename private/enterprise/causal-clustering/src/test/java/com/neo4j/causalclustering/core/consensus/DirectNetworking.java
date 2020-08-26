/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.database.DatabaseIdRepository;

public class DirectNetworking
{
    private final Map<RaftMemberId,com.neo4j.causalclustering.messaging.Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>>> handlers =
            new HashMap<>();
    private final Map<RaftMemberId,Queue<RaftMessages.InboundRaftMessageContainer<?>>> messageQueues = new HashMap<>();
    private final Set<RaftMemberId> disconnectedMembers = Collections.newSetFromMap( new ConcurrentHashMap<>() );
    private final RaftId raftId = RaftId.from( DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID.databaseId() );

    public void processMessages()
    {
        while ( messagesToBeProcessed() )
        {
            for ( Map.Entry<RaftMemberId,Queue<RaftMessages.InboundRaftMessageContainer<?>>> entry : messageQueues.entrySet() )
            {
                RaftMemberId id = entry.getKey();
                Queue<RaftMessages.InboundRaftMessageContainer<?>> queue = entry.getValue();
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
        for ( Queue<RaftMessages.InboundRaftMessageContainer<?>> queue : messageQueues.values() )
        {
            if ( !queue.isEmpty() )
            {
                return true;
            }
        }
        return false;
    }

    public void disconnect( RaftMemberId id )
    {
        disconnectedMembers.add( id );
    }

    public void reconnect( RaftMemberId id )
    {
        disconnectedMembers.remove( id );
    }

    public class Outbound implements com.neo4j.causalclustering.messaging.Outbound<RaftMemberId,RaftMessages.RaftMessage>
    {
        private final RaftMemberId me;

        public Outbound( RaftMemberId me )
        {
            this.me = me;
        }

        @Override
        public synchronized void send( RaftMemberId to, final RaftMessages.RaftMessage message, boolean block )
        {
            if ( canDeliver( to ) )
            {
                messageQueues.get( to ).add( RaftMessages.InboundRaftMessageContainer.of( Instant.now(), raftId, message ) );
            }
        }

        private boolean canDeliver( RaftMemberId to )
        {
            return messageQueues.containsKey( to ) &&
                    !disconnectedMembers.contains( to ) &&
                    !disconnectedMembers.contains( me );
        }
    }

    public class Inbound implements com.neo4j.causalclustering.messaging.Inbound<RaftMessages.InboundRaftMessageContainer<?>>
    {
        private final RaftMemberId id;

        public Inbound( RaftMemberId id )
        {
            this.id = id;
        }

        @Override
        public void registerHandler( MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> handler )
        {
            handlers.put( id, handler );
            messageQueues.put( id, new LinkedList<>() );
        }
    }
}
