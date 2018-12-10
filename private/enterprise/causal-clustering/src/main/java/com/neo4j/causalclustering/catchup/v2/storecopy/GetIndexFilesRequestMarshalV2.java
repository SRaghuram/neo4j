/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v2.storecopy;

import com.neo4j.causalclustering.catchup.v1.storecopy.GetIndexFilesRequest;
import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.List;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class GetIndexFilesRequestMarshalV2 extends SafeChannelMarshal<GetIndexFilesRequest>
{
    public static GetIndexFilesRequestMarshalV2 INSTANCE = new GetIndexFilesRequestMarshalV2();

    @Override
    protected GetIndexFilesRequest unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        //TODO: Factor out common stuff into super.marshall
        String databaseName = StringMarshal.unmarshal( channel );
        StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( channel );
        long requiredTransactionId = channel.getLong();
        long indexId = channel.getLong();
        return new GetIndexFilesRequest( storeId, indexId, requiredTransactionId, databaseName );
    }

    @Override
    public void marshal( GetIndexFilesRequest getIndexFilesRequest, WritableChannel channel ) throws IOException
    {
        StringMarshal.marshal( channel, getIndexFilesRequest.databaseName() );
        StoreIdMarshal.INSTANCE.marshal( getIndexFilesRequest.expectedStoreId(), channel );
        channel.putLong( getIndexFilesRequest.requiredTransactionId() );
        channel.putLong( getIndexFilesRequest.indexId() );
    }

    // TODO: Extract these or inline other encoders - consistency!
    // TODO: And also consider making encoders/decoders just be constructed as a wrapper around a marshaller.
    public static class Encoder extends MessageToByteEncoder<GetIndexFilesRequest>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, GetIndexFilesRequest msg, ByteBuf out ) throws Exception
        {
            GetIndexFilesRequestMarshalV2.INSTANCE.marshal( msg, new NetworkWritableChannel( out ) );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        @Override
        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            GetIndexFilesRequest getIndexFilesRequest = GetIndexFilesRequestMarshalV2.INSTANCE.unmarshal0( new NetworkReadableClosableChannelNetty4( in ) );
            out.add( getIndexFilesRequest );
        }
    }
}
