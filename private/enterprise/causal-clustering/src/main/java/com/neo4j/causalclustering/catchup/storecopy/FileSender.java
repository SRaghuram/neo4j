/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;
import java.util.Objects;

import org.neo4j.io.fs.StoreChannel;

import static com.neo4j.causalclustering.catchup.storecopy.FileChunk.MAX_PAYLOAD_SIZE;
import static com.neo4j.causalclustering.catchup.storecopy.FileSender.State.FINISHED;
import static com.neo4j.causalclustering.catchup.storecopy.FileSender.State.FULL_PENDING;
import static com.neo4j.causalclustering.catchup.storecopy.FileSender.State.LAST_PENDING;
import static com.neo4j.causalclustering.catchup.storecopy.FileSender.State.PRE_INIT;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;

class FileSender implements ChunkedInput<FileChunk>
{
    private final StoreResource resource;

    private StoreChannel channel;
    private ByteBuf nextPayload;
    private State state = PRE_INIT;

    FileSender( StoreResource resource )
    {
        this.resource = resource;
    }

    @Override
    public boolean isEndOfInput()
    {
        return state == FINISHED;
    }

    @Override
    public void close() throws Exception
    {
        if ( channel != null )
        {
            channel.close();
            channel = null;
        }
    }

    @Override
    public FileChunk readChunk( ByteBufAllocator allocator ) throws Exception
    {
        if ( state == FINISHED )
        {
            return null;
        }
        else if ( state == PRE_INIT )
        {
            channel = resource.open();
            nextPayload = prefetch( allocator );

            if ( nextPayload == null )
            {
                state = FINISHED;
                return FileChunk.create( EMPTY_BUFFER, true );
            }
            else
            {
                state = nextPayload.readableBytes() < MAX_PAYLOAD_SIZE ? LAST_PENDING : FULL_PENDING;
            }
        }

        if ( state == FULL_PENDING )
        {
            ByteBuf toSend = nextPayload;
            nextPayload = prefetch( allocator );

            if ( nextPayload == null )
            {
                state = FINISHED;
                return FileChunk.create( toSend, true );
            }
            else if ( nextPayload.readableBytes() < MAX_PAYLOAD_SIZE )
            {
                state = LAST_PENDING;
                return FileChunk.create( toSend, false );
            }
            else
            {
                return FileChunk.create( toSend, false );
            }
        }
        else if ( state == LAST_PENDING )
        {
            state = FINISHED;
            return FileChunk.create( nextPayload, true );
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    @Override
    public FileChunk readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public long length()
    {
        return -1;
    }

    @Override
    public long progress()
    {
        return 0;
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
        FileSender that = (FileSender) o;
        return Objects.equals( resource, that.resource );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( resource );
    }

    private ByteBuf prefetch( ByteBufAllocator allocator ) throws IOException
    {
        ByteBuf payload = allocator.ioBuffer( MAX_PAYLOAD_SIZE );

        int totalRead = 0;
        try
        {
            totalRead = read( payload );
        }
        finally
        {
            if ( totalRead == 0 )
            {
                payload.release();
                payload = null;
            }
        }

        return payload;
    }

    private int read( ByteBuf payload ) throws IOException
    {
        int totalRead = 0;
        do
        {
            int bytesReadOrEOF = payload.writeBytes( channel, MAX_PAYLOAD_SIZE - totalRead );
            if ( bytesReadOrEOF < 0 )
            {
                break;
            }
            totalRead += bytesReadOrEOF;
        }
        while ( totalRead < MAX_PAYLOAD_SIZE );
        return totalRead;
    }

    enum State
    {
        PRE_INIT,
        FULL_PENDING,
        LAST_PENDING,
        FINISHED
    }
}
