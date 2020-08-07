/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.core.consensus.NewLeaderBarrier;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSetSerializer;
import com.neo4j.causalclustering.core.replication.DistributedOperation;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseMarshalV2;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseRequest;
import com.neo4j.causalclustering.core.state.machines.status.StatusRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestMarshalV2;
import com.neo4j.causalclustering.core.state.machines.tx.ByteArrayReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.ChunkedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;
import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.List;

import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.kernel.database.DatabaseId;

public class ReplicatedContentCodec implements Codec<ReplicatedContent>
{
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
            output.add( ChunkedReplicatedContent.chunked( ContentCodes.TX_CONTENT_TYPE, replicatedTransaction.encode() ) );
        }

        @Override
        public void handle( TransactionRepresentationReplicatedTransaction replicatedTransaction )
        {
            output.add( ChunkedReplicatedContent.chunked( ContentCodes.TX_CONTENT_TYPE, replicatedTransaction.encode() ) );
        }

        @Override
        public void handle( MemberIdSet memberIdSet )
        {
            output.add( ChunkedReplicatedContent.single( ContentCodes.RAFT_MEMBER_SET_TYPE,
                    channel -> MemberIdSetSerializer.marshal( memberIdSet, channel ) ) );
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
        public void handle( ReplicatedLeaseRequest replicatedLeaseRequest )
        {
            output.add( ChunkedReplicatedContent.single( ContentCodes.LEASE_REQUEST,
                    channel -> ReplicatedLeaseMarshalV2.marshal( replicatedLeaseRequest, channel ) ) );
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

        @Override
        public void handle( StatusRequest statusRequest ) throws IOException
        {
            output.add(
                    ChunkedReplicatedContent.single( ContentCodes.STATUS_REQUEST, channel -> new StatusRequest.Marshal().marshal( statusRequest, channel ) ) );
        }
    }

    private ContentBuilder<ReplicatedContent> unmarshal( byte contentType, ByteBuf buffer ) throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case ContentCodes.TX_CONTENT_TYPE:
        {
            return ContentBuilder.finished( decodeTx( buffer ) );
        }
        case ContentCodes.DUMMY_REQUEST:
            return ContentBuilder.finished( DummyRequest.decode( buffer ) );
        default:
            return CoreReplicatedContentMarshal.unmarshal( contentType, new NetworkReadableChannel( buffer ) );
        }
    }

    /**
     * @see ChunkedTransaction
     * @see ByteArrayTransactionChunker
     */
    private static ReplicatedTransaction decodeTx( ByteBuf byteBuf ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( new NetworkReadableChannel( byteBuf ) );
        int length = byteBuf.readableBytes();
        byte[] bytes = new byte[length];
        byteBuf.readBytes( bytes );
        return ReplicatedTransaction.from( bytes, databaseId );
    }
}
