/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v1.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.List;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

// TODO: consider inlining marshalling into codecs
public class GetIndexFilesRequestMarshalV1 extends SafeChannelMarshal<GetIndexFilesRequest>
{
    private final String databaseName;

    public GetIndexFilesRequestMarshalV1( String databaseName )
    {
        this.databaseName = databaseName;
    }

    @Override
    protected GetIndexFilesRequest unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( channel );
        long requiredTransactionId = channel.getLong();
        long indexId = channel.getLong();
        return new GetIndexFilesRequest( storeId, indexId, requiredTransactionId, databaseName );
    }

    @Override
    public void marshal( GetIndexFilesRequest getIndexFilesRequest, WritableChannel channel ) throws IOException
    {
        StoreIdMarshal.INSTANCE.marshal( getIndexFilesRequest.expectedStoreId(), channel );
        channel.putLong( getIndexFilesRequest.requiredTransactionId() );
        channel.putLong( getIndexFilesRequest.indexId() );
    }

    // TODO: Move codec stuff somewhere V2 and don't sue V1 marshals?
    public static class Encoder extends MessageToByteEncoder<GetIndexFilesRequest>
    {
        @Override
        protected void encode( ChannelHandlerContext ctx, GetIndexFilesRequest msg, ByteBuf out ) throws Exception
        {
            // TODO: something better than null
            new GetIndexFilesRequestMarshalV1( null ).marshal( msg, new NetworkWritableChannel( out ) );
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
            GetIndexFilesRequest getIndexFilesRequest =
                    new GetIndexFilesRequestMarshalV1( databaseName ).unmarshal0( new NetworkReadableClosableChannelNetty4( in ) );
            out.add( getIndexFilesRequest );
        }
    }
}
