/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.List;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkWritableChannel;
import org.neo4j.causalclustering.messaging.StoreCopyRequest;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class GetIndexFilesRequest implements StoreCopyRequest
{
    private final StoreId expectedStoreId;
    private final long indexId;
    private final long requiredTransactionId;

    public GetIndexFilesRequest( StoreId expectedStoreId, long indexId, long requiredTransactionId )
    {
        this.expectedStoreId = expectedStoreId;
        this.indexId = indexId;
        this.requiredTransactionId = requiredTransactionId;
    }

    @Override
    public StoreId expectedStoreId()
    {
        return expectedStoreId;
    }

    @Override
    public long requiredTransactionId()
    {
        return requiredTransactionId;
    }

    public long indexId()
    {
        return indexId;
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.INDEX_SNAPSHOT;
    }

    static class IndexSnapshotRequestMarshall extends SafeChannelMarshal<GetIndexFilesRequest>
    {
        @Override
        protected GetIndexFilesRequest unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( channel );
            long requiredTransactionId = channel.getLong();
            long indexId = channel.getLong();
            return new GetIndexFilesRequest( storeId, indexId, requiredTransactionId );
        }

        @Override
        public void marshal( GetIndexFilesRequest getIndexFilesRequest, WritableChannel channel ) throws IOException
        {
            StoreIdMarshal.INSTANCE.marshal( getIndexFilesRequest.expectedStoreId(), channel );
            channel.putLong( getIndexFilesRequest.requiredTransactionId() );
            channel.putLong( getIndexFilesRequest.indexId() );
        }
    }

    public static class Encoder extends MessageToByteEncoder<GetIndexFilesRequest>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, GetIndexFilesRequest msg, ByteBuf out ) throws Exception
        {
            new IndexSnapshotRequestMarshall().marshal( msg, new NetworkWritableChannel( out ) );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        @Override
        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            GetIndexFilesRequest getIndexFilesRequest = new IndexSnapshotRequestMarshall().unmarshal0( new NetworkReadableClosableChannelNetty4( in ) );
            out.add( getIndexFilesRequest );
        }
    }

    @Override
    public String toString()
    {
        return "GetIndexFilesRequest{" + "expectedStoreId=" + expectedStoreId + ", indexId=" + indexId + ", requiredTransactionId=" + requiredTransactionId +
                '}';
    }
}
