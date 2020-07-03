/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import com.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.string.UTF8;

public class PrepareStoreCopyResponse
{
    public enum Status
    {
        SUCCESS,
        E_STORE_ID_MISMATCH,
        E_LISTING_STORE,
        E_DATABASE_UNKNOWN
    }

    private final Path[] paths;
    private final long lastCheckPointedTransactionId;
    private final Status status;

    public static PrepareStoreCopyResponse error( Status errorStatus )
    {
        if ( errorStatus == Status.SUCCESS )
        {
            throw new IllegalStateException( "Cannot create error result from state: " + errorStatus );
        }
        return new PrepareStoreCopyResponse( new Path[0], 0, errorStatus );
    }

    public static PrepareStoreCopyResponse success( Path[] storeFiles, long lastCheckPointedTransactionId )
    {
        return new PrepareStoreCopyResponse( storeFiles, lastCheckPointedTransactionId, Status.SUCCESS );
    }

    private PrepareStoreCopyResponse( Path[] paths, long lastCheckPointedTransactionId, Status status )
    {
        this.paths = paths;
        this.lastCheckPointedTransactionId = lastCheckPointedTransactionId;
        this.status = status;
    }

    public Path[] getPaths()
    {
        return paths;
    }

    public long lastCheckPointedTransactionId()
    {
        return lastCheckPointedTransactionId;
    }

    public Status status()
    {
        return status;
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
        PrepareStoreCopyResponse that = (PrepareStoreCopyResponse) o;
        return lastCheckPointedTransactionId == that.lastCheckPointedTransactionId &&
                status == that.status &&
                Arrays.equals( paths, that.paths );
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash( lastCheckPointedTransactionId, status );
        result = 31 * result + Arrays.hashCode( paths );
        return result;
    }

    public static class StoreListingMarshal extends SafeChannelMarshal<PrepareStoreCopyResponse>
    {
        @Override
        public void marshal( PrepareStoreCopyResponse prepareStoreCopyResponse, WritableChannel buffer ) throws IOException
        {
            buffer.putInt( prepareStoreCopyResponse.status.ordinal() );
            buffer.putLong( prepareStoreCopyResponse.lastCheckPointedTransactionId );
            marshalPaths( buffer, prepareStoreCopyResponse.paths );
        }

        @Override
        protected PrepareStoreCopyResponse unmarshal0( ReadableChannel channel ) throws IOException
        {
            int ordinal = channel.getInt();
            Status status = Status.values()[ordinal];
            long transactionId = channel.getLong();
            Path[] files = unmarshalPaths( channel );
            return new PrepareStoreCopyResponse( files, transactionId, status );
        }

        private static void marshalPaths( WritableChannel buffer, Path[] files ) throws IOException
        {
            buffer.putInt( files.length );
            for ( Path file : files )
            {
                putBytes( buffer, file.getFileName().toString() );
            }
        }

        private static Path[] unmarshalPaths( ReadableChannel channel ) throws IOException
        {
            int numberOfFiles = channel.getInt();
            Path[] paths = new Path[numberOfFiles];
            for ( int i = 0; i < numberOfFiles; i++ )
            {
                paths[i] = unmarshalPath( channel );
            }
            return paths;
        }

        private static Path unmarshalPath( ReadableChannel channel ) throws IOException
        {
            byte[] name = readBytes( channel );
            return Path.of( UTF8.decode( name ) );
        }

        private static void putBytes( WritableChannel buffer, String value ) throws IOException
        {
            byte[] bytes = UTF8.encode( value );
            buffer.putInt( bytes.length );
            buffer.put( bytes, bytes.length );
        }

        private static byte[] readBytes( ReadableChannel channel ) throws IOException
        {
            int bytesLength = channel.getInt();
            byte[] bytes = new byte[bytesLength];
            channel.get( bytes, bytesLength );
            return bytes;
        }
    }

    public static class Encoder extends MessageToByteEncoder<PrepareStoreCopyResponse>
    {

        @Override
        protected void encode( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyResponse prepareStoreCopyResponse, ByteBuf byteBuf )
                throws Exception
        {
            new PrepareStoreCopyResponse.StoreListingMarshal().marshal( prepareStoreCopyResponse, new BoundedNetworkWritableChannel( byteBuf ) );
        }
    }

    public static class Decoder extends ByteToMessageDecoder
    {

        @Override
        protected void decode( ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list ) throws Exception
        {
            list.add( new PrepareStoreCopyResponse.StoreListingMarshal().unmarshal( new NetworkReadableChannel( byteBuf ) ) );
        }
    }
}
