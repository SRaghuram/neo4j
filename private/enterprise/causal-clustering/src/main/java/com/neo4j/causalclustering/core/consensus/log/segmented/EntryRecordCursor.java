/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.neo4j.causalclustering.core.consensus.log.EntryRecord;
import com.neo4j.causalclustering.core.consensus.log.LogPosition;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.cursor.CursorValue;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;

import static org.neo4j.io.fs.ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;

/**
 * A cursor for iterating over RAFT log entries starting at an index and until the end of the segment is met.
 * The segment is demarcated by the ReadAheadChannel provided, which should properly signal the end of the channel.
 */
class EntryRecordCursor implements IOCursor<EntryRecord>
{
    private final ByteBuffer buffer;
    private ReadAheadChannel<StoreChannel> bufferedReader;

    private final LogPosition position;
    private final CursorValue<EntryRecord> currentRecord = new CursorValue<>();
    private final Reader reader;
    private ChannelMarshal<ReplicatedContent> contentMarshal;
    private final SegmentFile segment;

    private boolean hadError;
    private boolean closed;

    EntryRecordCursor( Reader reader, ChannelMarshal<ReplicatedContent> contentMarshal,
            long currentIndex, long wantedIndex, SegmentFile segment ) throws IOException, EndOfStreamException
    {
        this.buffer = ByteBuffers.allocateDirect( DEFAULT_READ_AHEAD_SIZE );
        this.bufferedReader = new ReadAheadChannel<>( reader.channel(), buffer );
        this.reader = reader;
        this.contentMarshal = contentMarshal;
        this.segment = segment;

        /* The cache lookup might have given us an earlier position, scan forward to the exact position. */
        while ( currentIndex < wantedIndex )
        {
            EntryRecord.read( bufferedReader, contentMarshal );
            currentIndex++;
        }

        this.position = new LogPosition( currentIndex, bufferedReader.position() );
    }

    @Override
    public boolean next() throws IOException
    {
        EntryRecord entryRecord;
        try
        {
            entryRecord = EntryRecord.read( bufferedReader, contentMarshal );
        }
        catch ( EndOfStreamException e )
        {
            currentRecord.invalidate();
            return false;
        }
        catch ( IOException e )
        {
            hadError = true;
            throw e;
        }

        currentRecord.set( entryRecord );
        position.byteOffset = bufferedReader.position();
        position.logIndex++;
        return true;
    }

    @Override
    public void close() throws IOException
    {
        if ( closed )
        {
            /* This is just a defensive measure, for catching user errors from messing up the refCount. */
            throw new IllegalStateException( "Already closed" );
        }

        bufferedReader = null;
        ByteBuffers.releaseBuffer( buffer );
        closed = true;
        segment.refCount().decrease();

        if ( hadError )
        {
            /* If the reader had en error, then it should be closed instead of returned to the pool. */
            reader.close();
        }
        else
        {
            segment.positionCache().put( position );
            segment.readerPool().release( reader );
        }
    }

    @Override
    public EntryRecord get()
    {
        return currentRecord.get();
    }
}
