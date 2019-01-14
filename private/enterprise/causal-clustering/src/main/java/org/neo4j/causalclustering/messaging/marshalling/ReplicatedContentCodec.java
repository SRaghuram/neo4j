/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

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
import org.neo4j.causalclustering.core.state.machines.tx.ChunkedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;

import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.DUMMY_REQUEST;
import static org.neo4j.causalclustering.messaging.marshalling.ContentCodes.TX_CONTENT_TYPE;

public class ReplicatedContentCodec implements Codec<ReplicatedContent>
{
    ReplicatedContentCodec()
    {
    }

    @Override
    public void encode( ReplicatedContent type, List<Object> output ) throws IOException
    {
        type.dispatch( new ChunkingHandler( output ) );
    }

    @Override
    public ContentBuilder<ReplicatedContent> decode( ByteBuf byteBuf ) throws IOException, EndOfStreamException
    {
        byte contentType = byteBuf.readByte();
        return unmarshal( contentType, byteBuf );
    }

    private static class ChunkingHandler implements ReplicatedContentHandler
    {
        private final List<Object> output;

        ChunkingHandler( List<Object> output )
        {
            this.output = output;
        }

        @Override
        public void handle( ByteArrayReplicatedTransaction replicatedTransaction )
        {
            output.add( ChunkedReplicatedContent.chunked( ContentCodes.TX_CONTENT_TYPE, new MaxTotalSize( replicatedTransaction.encode() ) ) );
        }

        @Override
        public void handle( TransactionRepresentationReplicatedTransaction replicatedTransaction )
        {
            output.add( ChunkedReplicatedContent.chunked( ContentCodes.TX_CONTENT_TYPE, new MaxTotalSize( replicatedTransaction.encode() ) ) );
        }

        @Override
        public void handle( MemberIdSet memberIdSet )
        {
            output.add( ChunkedReplicatedContent.single( ContentCodes.RAFT_MEMBER_SET_TYPE,
                    channel -> MemberIdSetSerializer.marshal( memberIdSet, channel ) ) );
        }

        @Override
        public void handle( ReplicatedIdAllocationRequest replicatedIdAllocationRequest )
        {
            output.add( ChunkedReplicatedContent.single( ContentCodes.ID_RANGE_REQUEST_TYPE,
                    channel -> ReplicatedIdAllocationRequestMarshalV2.marshal( replicatedIdAllocationRequest, channel ) ) );
        }

        @Override
        public void handle( ReplicatedTokenRequest replicatedTokenRequest )
        {
            output.add( ChunkedReplicatedContent.single( ContentCodes.TOKEN_REQUEST_TYPE,
                    channel -> ReplicatedTokenRequestMarshalV2.marshal( replicatedTokenRequest, channel ) ) );
        }

        @Override
        public void handle( NewLeaderBarrier newLeaderBarrier )
        {
            output.add( ChunkedReplicatedContent.single( ContentCodes.NEW_LEADER_BARRIER_TYPE, channel -> { } ) );
        }

        @Override
        public void handle( ReplicatedLockTokenRequest replicatedLockTokenRequest )
        {
            output.add( ChunkedReplicatedContent.single( ContentCodes.LOCK_TOKEN_REQUEST,
                    channel -> ReplicatedLockTokenMarshalV2.marshal( replicatedLockTokenRequest, channel ) ) );
        }

        @Override
        public void handle( DistributedOperation distributedOperation )
        {
            output.add( ChunkedReplicatedContent.single( ContentCodes.DISTRIBUTED_OPERATION, distributedOperation::marshalMetaData ) );
        }

        @Override
        public void handle( DummyRequest dummyRequest )
        {
            output.add( ChunkedReplicatedContent.chunked( ContentCodes.DUMMY_REQUEST, dummyRequest.encoder() ) );
        }
    }

    private ContentBuilder<ReplicatedContent> unmarshal( byte contentType, ByteBuf buffer ) throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case TX_CONTENT_TYPE:
        {
            return ContentBuilder.finished( decodeTx( buffer ) );
        }
        case DUMMY_REQUEST:
            return ContentBuilder.finished( DummyRequest.decode( buffer ) );
        default:
            return CoreReplicatedContentMarshalV2.unmarshal( contentType, new NetworkReadableClosableChannelNetty4( buffer ) );
        }
    }

    /**
     * @see ChunkedTransaction
     * @see ByteArrayTransactionChunker
     */
    private static ReplicatedTransaction decodeTx( ByteBuf byteBuf )
    {
        String databaseName = StringMarshal.unmarshal( byteBuf );
        int length = byteBuf.readableBytes();
        byte[] bytes = new byte[length];
        byteBuf.readBytes( bytes );
        return ReplicatedTransaction.from( bytes, databaseName );
    }
}
