/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.string.UTF8;

public class GetStoreFileRequestMarshalV2 extends SafeChannelMarshal<GetStoreFileRequest>
{
    public static GetStoreFileRequestMarshalV2 INSTANCE = new GetStoreFileRequestMarshalV2();

    @Override
    protected GetStoreFileRequest unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        String databaseName = StringMarshal.unmarshal( channel );
        StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( channel );
        long requiredTransactionId = channel.getLong();
        int fileNameLength = channel.getInt();
        byte[] fileNameBytes = new byte[fileNameLength];
        channel.get( fileNameBytes, fileNameLength );
        return new GetStoreFileRequest( storeId, new File( UTF8.decode( fileNameBytes ) ), requiredTransactionId, databaseName );
    }

    @Override
    public void marshal( GetStoreFileRequest getStoreFileRequest, WritableChannel channel ) throws IOException
    {
        StringMarshal.marshal( channel, getStoreFileRequest.databaseName() );
        StoreIdMarshal.INSTANCE.marshal( getStoreFileRequest.expectedStoreId(), channel );
        channel.putLong( getStoreFileRequest.requiredTransactionId() );
        String name = getStoreFileRequest.file().getName();
        channel.putInt( name.length() );
        channel.put( UTF8.encode( name ), name.length() );
    }

    public static class Encoder extends MessageToByteEncoder<GetStoreFileRequest>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, GetStoreFileRequest msg, ByteBuf out ) throws Exception
        {
            new GetStoreFileRequestMarshalV2().marshal( msg, new NetworkWritableChannel( out ) );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        @Override
        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            GetStoreFileRequest getStoreFileRequest = new GetStoreFileRequestMarshalV2().unmarshal0( new NetworkReadableClosableChannelNetty4( in ) );
            out.add( getStoreFileRequest );
        }
    }
}
