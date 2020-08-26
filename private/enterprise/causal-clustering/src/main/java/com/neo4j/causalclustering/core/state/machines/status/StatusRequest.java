/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.status;

import com.neo4j.causalclustering.core.state.CommandDispatcher;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.core.state.machines.NoOperationRequest;
import com.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;
import com.neo4j.causalclustering.messaging.marshalling.UUIDMarshal;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Consumer;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;
import org.neo4j.kernel.database.DatabaseId;

public class StatusRequest implements CoreReplicatedContent, NoOperationRequest
{
    private final UUID messageId;
    private final DatabaseId databaseId;
    private final RaftMemberId memberId;

    public StatusRequest( UUID messageId, DatabaseId databaseId, RaftMemberId memberId )
    {
        this.messageId = messageId;
        this.databaseId = databaseId;
        this.memberId = memberId;
    }

    @Override
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<StateMachineResult> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public RaftMemberId memberId()
    {
        return memberId;
    }

    public UUID messageId()
    {
        return messageId;
    }

    @Override
    public void dispatch( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }

    public static class Marshal extends SafeChannelMarshal<StatusRequest>
    {
        public static final SafeChannelMarshal<StatusRequest> INSTANCE = new Marshal();

        @Override
        protected StatusRequest unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            return new StatusRequest( UUIDMarshal.INSTANCE.unmarshal( channel ), DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel ),
                    RaftMemberId.Marshal.INSTANCE.unmarshal0( channel ) );
        }

        @Override
        public void marshal( StatusRequest statusRequest, WritableChannel channel ) throws IOException
        {
            UUIDMarshal.INSTANCE.marshal( statusRequest.messageId, channel );
            DatabaseIdWithoutNameMarshal.INSTANCE.marshal( statusRequest.databaseId, channel );
            RaftMemberId.Marshal.INSTANCE.marshal( statusRequest.memberId, channel );
        }
    }
}
