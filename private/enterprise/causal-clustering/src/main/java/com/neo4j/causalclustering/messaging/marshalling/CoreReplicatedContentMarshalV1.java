/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequestMarshalV1;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenMarshalV1;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestMarshalV1;
import com.neo4j.causalclustering.core.state.machines.tx.ByteArrayReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionMarshalV1;
import com.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;
import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.messaging.EndOfStreamException;

import java.io.IOException;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static com.neo4j.causalclustering.messaging.marshalling.ContentCodes.DISTRIBUTED_OPERATION;
import static com.neo4j.causalclustering.messaging.marshalling.ContentCodes.DUMMY_REQUEST;
import static com.neo4j.causalclustering.messaging.marshalling.ContentCodes.ID_RANGE_REQUEST_TYPE;
import static com.neo4j.causalclustering.messaging.marshalling.ContentCodes.LOCK_TOKEN_REQUEST;
import static com.neo4j.causalclustering.messaging.marshalling.ContentCodes.NEW_LEADER_BARRIER_TYPE;
import static com.neo4j.causalclustering.messaging.marshalling.ContentCodes.RAFT_MEMBER_SET_TYPE;
import static com.neo4j.causalclustering.messaging.marshalling.ContentCodes.TOKEN_REQUEST_TYPE;
import static com.neo4j.causalclustering.messaging.marshalling.ContentCodes.TX_CONTENT_TYPE;

public class CoreReplicatedContentMarshalV1 extends SafeChannelMarshal<ReplicatedContent>
{
    private final String databaseName;

    /**
     * @param databaseName The assumed database name when unmarshalling from a V1 remote.
     */
    CoreReplicatedContentMarshalV1( String databaseName )
    {
        this.databaseName = databaseName;
    }

    @Override
    public void marshal( ReplicatedContent replicatedContent, WritableChannel channel ) throws IOException
    {
        replicatedContent.dispatch( new MarshallingHandler( channel ) );
    }

    @Override
    protected ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        assert databaseName != null;

        byte type = channel.get();
        ContentBuilder<ReplicatedContent> contentBuilder = unmarshal( type, channel, databaseName );
        while ( !contentBuilder.isComplete() )
        {
            type = channel.get();
            contentBuilder = contentBuilder.combine( unmarshal( type, channel, databaseName ) );
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
            writableChannel.put( TX_CONTENT_TYPE );
            ReplicatedTransactionMarshalV1.marshal( writableChannel, tx );
        }

        @Override
        public void handle( TransactionRepresentationReplicatedTransaction tx ) throws IOException
        {
            writableChannel.put( TX_CONTENT_TYPE );
            ReplicatedTransactionMarshalV1.marshal( writableChannel, tx );
        }

        @Override
        public void handle( MemberIdSet memberIdSet ) throws IOException
        {
            writableChannel.put( RAFT_MEMBER_SET_TYPE );
            MemberIdSetSerializer.marshal( memberIdSet, writableChannel );
        }

        @Override
        public void handle( ReplicatedIdAllocationRequest replicatedIdAllocationRequest ) throws IOException
        {
            writableChannel.put( ID_RANGE_REQUEST_TYPE );
            ReplicatedIdAllocationRequestMarshalV1.marshal( replicatedIdAllocationRequest, writableChannel );
        }

        @Override
        public void handle( ReplicatedTokenRequest replicatedTokenRequest ) throws IOException
        {
            writableChannel.put( TOKEN_REQUEST_TYPE );
            ReplicatedTokenRequestMarshalV1.marshal( replicatedTokenRequest, writableChannel );
        }

        @Override
        public void handle( NewLeaderBarrier newLeaderBarrier ) throws IOException
        {
            writableChannel.put( NEW_LEADER_BARRIER_TYPE );
        }

        @Override
        public void handle( ReplicatedLockTokenRequest replicatedLockTokenRequest ) throws IOException
        {
            writableChannel.put( LOCK_TOKEN_REQUEST );
            ReplicatedLockTokenMarshalV1.marshal( replicatedLockTokenRequest, writableChannel );
        }

        @Override
        public void handle( DistributedOperation distributedOperation ) throws IOException
        {
            writableChannel.put( DISTRIBUTED_OPERATION );
            distributedOperation.marshalMetaData( writableChannel );
        }

        @Override
        public void handle( DummyRequest dummyRequest ) throws IOException
        {
            writableChannel.put( DUMMY_REQUEST );
            DummyRequest.Marshal.INSTANCE.marshal( dummyRequest, writableChannel );
        }
    }

    public static ContentBuilder<ReplicatedContent> unmarshal( byte contentType, ReadableChannel channel, String databaseName )
            throws IOException, EndOfStreamException
    {
        switch ( contentType )
        {
        case TX_CONTENT_TYPE:
            return ContentBuilder.finished( ReplicatedTransactionMarshalV1.unmarshal( channel, databaseName ) );
        case RAFT_MEMBER_SET_TYPE:
            return ContentBuilder.finished( MemberIdSetSerializer.unmarshal( channel ) );
        case ID_RANGE_REQUEST_TYPE:
            return ContentBuilder.finished( ReplicatedIdAllocationRequestMarshalV1.unmarshal( channel, databaseName ) );
        case TOKEN_REQUEST_TYPE:
            return ContentBuilder.finished( ReplicatedTokenRequestMarshalV1.unmarshal( channel, databaseName ) );
        case NEW_LEADER_BARRIER_TYPE:
            return ContentBuilder.finished( new NewLeaderBarrier() );
        case LOCK_TOKEN_REQUEST:
            return ContentBuilder.finished( ReplicatedLockTokenMarshalV1.unmarshal( channel, databaseName ) );
        case DISTRIBUTED_OPERATION:
            return DistributedOperation.deserialize( channel );
        case DUMMY_REQUEST:
            return ContentBuilder.finished( DummyRequest.Marshal.INSTANCE.unmarshal( channel ) );
        default:
            throw new IllegalStateException( "Not a recognized content type: " + contentType );
        }
    }
}
