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
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionMarshalV2;
import com.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeChannelMarshal;

public class CoreReplicatedContentMarshal extends SafeChannelMarshal<ReplicatedContent>
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
        public void handle( ReplicatedLeaseRequest replicatedLeaseRequest ) throws IOException
        {
            writableChannel.put( ContentCodes.LEASE_REQUEST );
            ReplicatedLeaseMarshalV2.marshal( replicatedLeaseRequest, writableChannel );
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

        @Override
        public void handle( StatusRequest statusRequest ) throws IOException
        {
            writableChannel.put( ContentCodes.STATUS_REQUEST );
            new StatusRequest.Marshal().marshal( statusRequest, writableChannel );
        }
    }

    public static ContentBuilder<ReplicatedContent> unmarshal( byte contentType, ReadableChannel channel )
            throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case ContentCodes.TX_CONTENT_TYPE:
            return ContentBuilder.finished( ReplicatedTransactionMarshalV2.unmarshal( channel ) );
        case ContentCodes.RAFT_MEMBER_SET_TYPE:
            return ContentBuilder.finished( MemberIdSetSerializer.unmarshal( channel ) );
        case ContentCodes.TOKEN_REQUEST_TYPE:
            return ContentBuilder.finished( ReplicatedTokenRequestMarshalV2.unmarshal( channel ) );
        case ContentCodes.NEW_LEADER_BARRIER_TYPE:
            return ContentBuilder.finished( new NewLeaderBarrier() );
        case ContentCodes.LEASE_REQUEST:
            return ContentBuilder.finished( ReplicatedLeaseMarshalV2.unmarshal( channel ) );
        case ContentCodes.DISTRIBUTED_OPERATION:
            return DistributedOperation.deserialize( channel );
        case ContentCodes.DUMMY_REQUEST:
            return ContentBuilder.finished( DummyRequest.Marshal.INSTANCE.unmarshal( channel ) );
        case ContentCodes.STATUS_REQUEST:
            return ContentBuilder.finished( StatusRequest.Marshal.INSTANCE.unmarshal( channel ) );
        default:
            throw new IllegalStateException( "Not a recognized content type: " + contentType );
        }
    }
}
