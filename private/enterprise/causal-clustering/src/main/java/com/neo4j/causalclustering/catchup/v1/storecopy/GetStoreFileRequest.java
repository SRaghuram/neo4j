/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v1.storecopy;

import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.StoreCopyRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.File;
import java.util.List;

public class GetStoreFileRequest implements StoreCopyRequest
{
    private final StoreId expectedStoreId;
    private final File file;
    private final long requiredTransactionId;
    private final String databaseName;

    public GetStoreFileRequest( StoreId expectedStoreId, File file, long requiredTransactionId, String databaseName )
    {
        this.expectedStoreId = expectedStoreId;
        this.file = file;
        this.requiredTransactionId = requiredTransactionId;
        this.databaseName = databaseName;
    }

    @Override
    public long requiredTransactionId()
    {
        return requiredTransactionId;
    }

    @Override
    public StoreId expectedStoreId()
    {
        return expectedStoreId;
    }

    public File file()
    {
        return file;
    }

    @Override
    public RequestMessageType messageType()
    {
        return RequestMessageType.STORE_FILE;
    }

    @Override
    public String databaseName()
    {
        return databaseName;
    }

    public static class Encoder extends MessageToByteEncoder<GetStoreFileRequest>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, GetStoreFileRequest msg, ByteBuf out ) throws Exception
        {
            // TODO: something better than null
            new GetStoreFileRequestMarshalV1( null ).marshal( msg, new NetworkWritableChannel( out ) );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {
        private String databaseName;

        public Decoder( String databaseName )
        {
            this.databaseName = databaseName;
        }

        @Override
        protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
        {
            GetStoreFileRequest getStoreFileRequest =
                    new GetStoreFileRequestMarshalV1( databaseName ).unmarshal0( new NetworkReadableClosableChannelNetty4( in ) );
            out.add( getStoreFileRequest );
        }
    }

    @Override
    public String toString()
    {
        return "GetStoreFileRequest{" + "expectedStoreId=" + expectedStoreId + ", file=" + file.getName() + ", requiredTransactionId=" + requiredTransactionId +
                '}';
    }
}
