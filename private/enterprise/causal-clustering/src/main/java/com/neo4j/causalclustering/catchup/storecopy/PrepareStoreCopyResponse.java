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

import java.io.File;
import java.io.IOException;
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

    private final File[] files;
    private final long lastCheckPointedTransactionId;
    private final Status status;

    public static PrepareStoreCopyResponse error( Status errorStatus )
    {
        if ( errorStatus == Status.SUCCESS )
        {
            throw new IllegalStateException( "Cannot create error result from state: " + errorStatus );
        }
        return new PrepareStoreCopyResponse( new File[0], 0, errorStatus );
    }

    public static PrepareStoreCopyResponse success( File[] storeFiles, long lastCheckPointedTransactionId )
    {
        return new PrepareStoreCopyResponse( storeFiles, lastCheckPointedTransactionId, Status.SUCCESS );
    }

    private PrepareStoreCopyResponse( File[] files, long lastCheckPointedTransactionId, Status status )
    {
        this.files = files;
        this.lastCheckPointedTransactionId = lastCheckPointedTransactionId;
        this.status = status;
    }

    public File[] getFiles()
    {
        return files;
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
               Arrays.equals( files, that.files ) &&
               status == that.status;
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash( lastCheckPointedTransactionId, status );
        result = 31 * result + Arrays.hashCode( files );
        return result;
    }

    public static class StoreListingMarshal extends SafeChannelMarshal<PrepareStoreCopyResponse>
    {
        @Override
        public void marshal( PrepareStoreCopyResponse prepareStoreCopyResponse, WritableChannel buffer ) throws IOException
        {
            buffer.putInt( prepareStoreCopyResponse.status.ordinal() );
            buffer.putLong( prepareStoreCopyResponse.lastCheckPointedTransactionId );
            marshalFiles( buffer, prepareStoreCopyResponse.files );
        }

        @Override
        protected PrepareStoreCopyResponse unmarshal0( ReadableChannel channel ) throws IOException
        {
            int ordinal = channel.getInt();
            Status status = Status.values()[ordinal];
            long transactionId = channel.getLong();
            File[] files = unmarshalFiles( channel );
            return new PrepareStoreCopyResponse( files, transactionId, status );
        }

        private static void marshalFiles( WritableChannel buffer, File[] files ) throws IOException
        {
            buffer.putInt( files.length );
            for ( File file : files )
            {
                putBytes( buffer, file.getName() );
            }
        }

        private static File[] unmarshalFiles( ReadableChannel channel ) throws IOException
        {
            int numberOfFiles = channel.getInt();
            File[] files = new File[numberOfFiles];
            for ( int i = 0; i < numberOfFiles; i++ )
            {
                files[i] = unmarshalFile( channel );
            }
            return files;
        }

        private static File unmarshalFile( ReadableChannel channel ) throws IOException
        {
            byte[] name = readBytes( channel );
            return new File( UTF8.decode( name ) );
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
