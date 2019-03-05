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
import java.util.Objects;

public class GetStoreFileRequest extends StoreCopyRequest
{
    private final File file;

    public GetStoreFileRequest( StoreId expectedStoreId, File file, long requiredTransactionId, String databaseName )
    {
        super( RequestMessageType.STORE_FILE, databaseName, expectedStoreId, requiredTransactionId );
        this.file = file;
    }

    public File file()
    {
        return file;
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
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        if ( !super.equals( o ) )
        {
            return false;
        }
        GetStoreFileRequest that = (GetStoreFileRequest) o;
        return Objects.equals( file, that.file );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), file );
    }

    @Override
    public String toString()
    {
        return "GetStoreFileRequest{" +
               "expectedStoreId=" + expectedStoreId() +
               ", file=" + file.getName() +
               ", requiredTransactionId=" + requiredTransactionId() +
               ", databaseName=" + databaseName() +
               "}";
    }
}
