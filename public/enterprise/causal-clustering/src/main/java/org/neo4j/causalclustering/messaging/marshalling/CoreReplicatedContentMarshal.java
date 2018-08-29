/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSetSerializer;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenSerializer;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestSerializer;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionSerializer;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.causalclustering.messaging.marshalling.ChunkedEncoder.single;

public class CoreReplicatedContentMarshal extends SafeChannelMarshal<ReplicatedContent>
{
    private static final byte TX_CONTENT_TYPE = 0;
    private static final byte RAFT_MEMBER_SET_TYPE = 1;
    private static final byte ID_RANGE_REQUEST_TYPE = 2;
    private static final byte TOKEN_REQUEST_TYPE = 4;
    private static final byte NEW_LEADER_BARRIER_TYPE = 5;
    private static final byte LOCK_TOKEN_REQUEST = 6;
    private static final byte DISTRIBUTED_OPERATION = 7;
    private static final byte DUMMY_REQUEST = 8;

    public void marshal( ReplicatedContent content, Consumer<ChunkedReplicatedContent> consumer )
    {
        if ( content instanceof ReplicatedTransaction )
        {
            consumer.accept(
                    new ChunkedReplicatedContent( TX_CONTENT_TYPE, ((ReplicatedTransaction) content).marshal(), ByteBufChunkHandler.maxSizeHandler() ) );
        }
        else if ( content instanceof MemberIdSet )
        {
            consumer.accept( new ChunkedReplicatedContent( RAFT_MEMBER_SET_TYPE,
                    single( channel -> MemberIdSetSerializer.marshal( (MemberIdSet) content, channel ) ) ) );
        }
        else if ( content instanceof ReplicatedIdAllocationRequest )
        {
            consumer.accept( new ChunkedReplicatedContent( ID_RANGE_REQUEST_TYPE,
                    single( channel -> ReplicatedIdAllocationRequestSerializer.marshal( (ReplicatedIdAllocationRequest) content, channel ) ) ) );
        }
        else if ( content instanceof ReplicatedTokenRequest )
        {
            consumer.accept( new ChunkedReplicatedContent( TOKEN_REQUEST_TYPE,
                    single( channel -> ReplicatedTokenRequestSerializer.marshal( (ReplicatedTokenRequest) content, channel ) ) ) );
        }
        else if ( content instanceof NewLeaderBarrier )
        {
            consumer.accept( new ChunkedReplicatedContent( NEW_LEADER_BARRIER_TYPE, single( channel ->
            {
            } ) ) );
        }
        else if ( content instanceof ReplicatedLockTokenRequest )
        {
            consumer.accept( new ChunkedReplicatedContent( LOCK_TOKEN_REQUEST,
                    single( channel -> ReplicatedLockTokenSerializer.marshal( (ReplicatedLockTokenRequest) content, channel ) ) ) );
        }
        else if ( content instanceof DistributedOperation )
        {
            consumer.accept( new ChunkedReplicatedContent( DISTRIBUTED_OPERATION, ((DistributedOperation) content).serialize() ) );
            marshal( ((DistributedOperation) content).content(), consumer );
        }
        else if ( content instanceof DummyRequest )
        {
            consumer.accept( new ChunkedReplicatedContent( DUMMY_REQUEST, ((DummyRequest) content).serializer() ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown content type " + content.getClass() );
        }
    }

    public ContentBuilder<ReplicatedContent> unmarshalContent( byte contentType, ByteBuf buffer ) throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case TX_CONTENT_TYPE:
        {
            return ContentBuilder.finished( ReplicatedTransactionSerializer.decode( buffer ) );
        }
        default:
            return unmarshalContent( contentType, new NetworkReadableClosableChannelNetty4( buffer ) );
        }
    }

    private ContentBuilder<ReplicatedContent> unmarshalContent( byte contentType, ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case TX_CONTENT_TYPE:
            return ContentBuilder.finished( ReplicatedTransactionSerializer.unmarshal( channel ) );
        case RAFT_MEMBER_SET_TYPE:
            return ContentBuilder.finished( MemberIdSetSerializer.unmarshal( channel ) );
        case ID_RANGE_REQUEST_TYPE:
            return ContentBuilder.finished( ReplicatedIdAllocationRequestSerializer.unmarshal( channel ) );
        case TOKEN_REQUEST_TYPE:
            return ContentBuilder.finished( ReplicatedTokenRequestSerializer.unmarshal( channel ) );
        case NEW_LEADER_BARRIER_TYPE:
            return ContentBuilder.finished( new NewLeaderBarrier() );
        case LOCK_TOKEN_REQUEST:
            return ContentBuilder.finished( ReplicatedLockTokenSerializer.unmarshal( channel ) );
        case DISTRIBUTED_OPERATION:
        {
            return DistributedOperation.deserialize( channel );
        }
        case DUMMY_REQUEST:
            return ContentBuilder.finished( DummyRequest.Marshal.INSTANCE.unmarshal( channel ) );
        default:
            throw new IllegalStateException( "Not a recognized content type: " + contentType );
        }
    }

    @Override
    public void marshal( ReplicatedContent replicatedContent, WritableChannel channel ) throws IOException
    {
        ArrayList<Marshal> marshaler = new ArrayList<>();
        marshal( replicatedContent, marshaler::add );
        for ( Marshal marshal : marshaler )
        {
            marshal.marshal( channel );
        }
    }

    @Override
    protected ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        byte type = channel.get();
        ContentBuilder<ReplicatedContent> contentBuilder = unmarshalContent( type, channel );
        while ( !contentBuilder.isComplete() )
        {
            type = channel.get();
            contentBuilder = contentBuilder.combine( unmarshalContent( type, channel ) );
        }
        return contentBuilder.build();
    }
}
