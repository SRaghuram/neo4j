/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSetSerializer;
import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequestMarshalV2;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenMarshalV2;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestMarshalV2;
import org.neo4j.causalclustering.core.state.machines.tx.ByteArrayReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionMarshalV2;
import org.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.DISTRIBUTED_OPERATION;
import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.DUMMY_REQUEST;
import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.ID_RANGE_REQUEST_TYPE;
import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.LOCK_TOKEN_REQUEST;
import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.NEW_LEADER_BARRIER_TYPE;
import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.RAFT_MEMBER_SET_TYPE;
import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.TOKEN_REQUEST_TYPE;
import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.TX_CONTENT_TYPE;

public class CoreReplicatedContentMarshalV2 extends SafeChannelMarshal<ReplicatedContent>
{
    @Override
    public void marshal( ReplicatedContent replicatedContent, WritableChannel channel ) throws IOException
    {
        replicatedContent.dispatch( new MarshallingHandler( channel ) );
    }

    @Override
    protected ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        byte type = channel.get();
        ContentBuilder<ReplicatedContent> contentBuilder = unmarshal( type, channel );
        while ( !contentBuilder.isComplete() )
        {
            type = channel.get();
            contentBuilder = contentBuilder.combine( unmarshal( type, channel ) );
        }
        return contentBuilder.build();
    }

    private static class MarshallingHandler implements ReplicatedContentHandler
    {
        private final WritableChannel writableChannel;

        MarshallingHandler( WritableChannel writableChannel )
        {
            this.writableChannel = writableChannel;
        }

        @Override
        public void handle( ByteArrayReplicatedTransaction tx ) throws IOException
        {
            writableChannel.put( ContentCodes.TX_CONTENT_TYPE );
            ReplicatedTransactionMarshalV2.marshal( writableChannel, tx );
        }

        @Override
        public void handle( TransactionRepresentationReplicatedTransaction tx ) throws IOException
        {
            writableChannel.put( ContentCodes.TX_CONTENT_TYPE );
            ReplicatedTransactionMarshalV2.marshal( writableChannel, tx );
        }

        @Override
        public void handle( MemberIdSet memberIdSet ) throws IOException
        {
            writableChannel.put( ContentCodes.RAFT_MEMBER_SET_TYPE );
            MemberIdSetSerializer.marshal( memberIdSet, writableChannel );
        }

        @Override
        public void handle( ReplicatedIdAllocationRequest replicatedIdAllocationRequest ) throws IOException
        {
            writableChannel.put( ContentCodes.ID_RANGE_REQUEST_TYPE );
            ReplicatedIdAllocationRequestMarshalV2.marshal( replicatedIdAllocationRequest, writableChannel );
        }

        @Override
        public void handle( ReplicatedTokenRequest replicatedTokenRequest ) throws IOException
        {
            writableChannel.put( ContentCodes.TOKEN_REQUEST_TYPE );
            ReplicatedTokenRequestMarshalV2.marshal( replicatedTokenRequest, writableChannel );
        }

        @Override
        public void handle( NewLeaderBarrier newLeaderBarrier ) throws IOException
        {
            writableChannel.put( ContentCodes.NEW_LEADER_BARRIER_TYPE );
        }

        @Override
        public void handle( ReplicatedLockTokenRequest replicatedLockTokenRequest ) throws IOException
        {
            writableChannel.put( ContentCodes.LOCK_TOKEN_REQUEST );
            ReplicatedLockTokenMarshalV2.marshal( replicatedLockTokenRequest, writableChannel );
        }

        @Override
        public void handle( DistributedOperation distributedOperation ) throws IOException
        {
            writableChannel.put( ContentCodes.DISTRIBUTED_OPERATION );
            distributedOperation.marshalMetaData( writableChannel );
        }

        @Override
        public void handle( DummyRequest dummyRequest ) throws IOException
        {
            writableChannel.put( ContentCodes.DUMMY_REQUEST );
            DummyRequest.Marshal.INSTANCE.marshal( dummyRequest, writableChannel );
        }
    }

    public static ContentBuilder<ReplicatedContent> unmarshal( byte contentType, ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case TX_CONTENT_TYPE:
            return ContentBuilder.finished( ReplicatedTransactionMarshalV2.unmarshal( channel ) );
        case RAFT_MEMBER_SET_TYPE:
            return ContentBuilder.finished( MemberIdSetSerializer.unmarshal( channel ) );
        case ID_RANGE_REQUEST_TYPE:
            return ContentBuilder.finished( ReplicatedIdAllocationRequestMarshalV2.unmarshal( channel ) );
        case TOKEN_REQUEST_TYPE:
            return ContentBuilder.finished( ReplicatedTokenRequestMarshalV2.unmarshal( channel ) );
        case NEW_LEADER_BARRIER_TYPE:
            return ContentBuilder.finished( new NewLeaderBarrier() );
        case LOCK_TOKEN_REQUEST:
            return ContentBuilder.finished( ReplicatedLockTokenMarshalV2.unmarshal( channel ) );
        case DISTRIBUTED_OPERATION:
            return DistributedOperation.deserialize( channel );
        case DUMMY_REQUEST:
            return ContentBuilder.finished( DummyRequest.Marshal.INSTANCE.unmarshal( channel ) );
        default:
            throw new IllegalStateException( "Not a recognized content type: " + contentType );
        }
    }
}
