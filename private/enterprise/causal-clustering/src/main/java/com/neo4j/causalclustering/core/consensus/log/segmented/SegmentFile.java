/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.EntryRecord;
import com.neo4j.causalclustering.core.consensus.log.LogPosition;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;

import java.io.File;
import java.io.IOException;

import org.neo4j.cursor.EmptyIOCursor;
import org.neo4j.cursor.IOCursor;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

import static java.lang.String.format;

/**
 * Keeps track of a segment of the RAFT log, i.e. a consecutive set of entries.
 * Concurrent reading is thread-safe.
 */
class SegmentFile implements AutoCloseable
{
    private static final SegmentHeader.Marshal headerMarshal = new SegmentHeader.Marshal();

    private final Log log;
    private final FileSystemAbstraction fileSystem;
    private final File file;
    private final ReaderPool readerPool;
    private final ChannelMarshal<ReplicatedContent> contentMarshal;

    private final PositionCache positionCache;
    private final MemoryTracker memoryTracker;
    private final ReferenceCounter refCount;

    private final SegmentHeader header;
    private final long version;

    private PhysicalFlushableChannel bufferedWriter;

    SegmentFile( FileSystemAbstraction fileSystem, File file, ReaderPool readerPool, long version,
            ChannelMarshal<ReplicatedContent> contentMarshal, LogProvider logProvider, SegmentHeader header, MemoryTracker memoryTracker )
    {
        this.fileSystem = fileSystem;
        this.file = file;
        this.readerPool = readerPool;
        this.contentMarshal = contentMarshal;
        this.header = header;
        this.version = version;

        this.positionCache = new PositionCache( header.recordOffset() );
        this.memoryTracker = memoryTracker;
        this.refCount = new ReferenceCounter();

        this.log = logProvider.getLog( getClass() );
    }

    static SegmentFile create( FileSystemAbstraction fileSystem, File file, ReaderPool readerPool, long version,
            ChannelMarshal<ReplicatedContent> contentMarshal, LogProvider logProvider, SegmentHeader header, MemoryTracker memoryTracker )
            throws IOException
    {
        if ( fileSystem.fileExists( file ) )
        {
            throw new IllegalStateException( "File was not expected to exist" );
        }

        SegmentFile segment = new SegmentFile( fileSystem, file, readerPool, version, contentMarshal, logProvider, header, memoryTracker );
        headerMarshal.marshal( header, segment.getOrCreateWriter() );
        segment.flush();

        return segment;
    }

    /**
     * Channels must be closed when no longer used, so that they are released back to the pool of readers.
     *
     */
    IOCursor<EntryRecord> getCursor( long logIndex ) throws IOException, DisposedException
    {
        assert logIndex > header.prevIndex();

        if ( !refCount.increase() )
        {
            throw new DisposedException();
        }

        /* This is the relative index within the file, starting from zero. */
        long offsetIndex = logIndex - (header.prevIndex() + 1);

        LogPosition position = positionCache.lookup( offsetIndex );
        Reader reader = readerPool.acquire( version, position.byteOffset );

        try
        {
            long currentIndex = position.logIndex;
            return new EntryRecordCursor( reader, contentMarshal, currentIndex, offsetIndex, this, memoryTracker );
        }
        catch ( EndOfStreamException e )
        {
            readerPool.release( reader );
            refCount.decrease();
            return EmptyIOCursor.empty();
        }
        catch ( IOException e )
        {
            reader.close();
            refCount.decrease();
            throw e;
        }
    }

    private synchronized PhysicalFlushableChannel getOrCreateWriter() throws IOException
    {
        if ( bufferedWriter == null )
        {
            if ( !refCount.increase() )
            {
                throw new IOException( "Writer has been closed" );
            }

            StoreChannel channel = fileSystem.write( file );
            channel.position( channel.size() );
            bufferedWriter = new PhysicalFlushableChannel( channel, new NativeScopedBuffer( ByteUnit.kibiBytes( 512 ), memoryTracker ) );
        }
        return bufferedWriter;
    }

    synchronized long position() throws IOException
    {
        return getOrCreateWriter().position();
    }

    /**
     * Idempotently closes the writer.
     *
     */
    synchronized void closeWriter()
    {
        if ( bufferedWriter != null )
        {
            try
            {
                flush();
                bufferedWriter.close();
            }
            catch ( IOException e )
            {
                log.error( "Failed to close writer for: " + file, e );
            }
            finally
            {
                bufferedWriter = null;
                refCount.decrease();
            }
        }
    }

    public synchronized void write( long logIndex, RaftLogEntry entry ) throws IOException
    {
        EntryRecord.write( getOrCreateWriter(), contentMarshal, logIndex, entry.term(), entry.content() );
    }

    synchronized void flush() throws IOException
    {
        bufferedWriter.prepareForFlush().flush();
    }

    public boolean delete()
    {
        return fileSystem.deleteFile( file );
    }

    public SegmentHeader header()
    {
        return header;
    }

    public long size()
    {
        return fileSystem.getFileSize( file );
    }

    String getFilename()
    {
        return file.getName();
    }

    /**
     * Called by the pruner when it wants to prune this segment. If there are no open
     * readers or writers then the segment will be closed.
     *
     * @return True if the segment can be pruned at this time, false otherwise.
     */
    boolean tryClose()
    {
        if ( refCount.tryDispose() )
        {
            close();
            return true;
        }
        return false;
    }

    @Override
    public void close()
    {
        closeWriter();
        readerPool.prune( version );

        if ( !refCount.tryDispose() )
        {
            throw new IllegalStateException( format( "Segment still referenced. Value: %d", refCount.get() ) );
        }
    }

    @Override
    public String toString()
    {
        return "SegmentFile{" +
               "file=" + file.getName() +
               ", header=" + header +
               '}';
    }

    ReferenceCounter refCount()
    {
        return refCount;
    }

    PositionCache positionCache()
    {
        return positionCache;
    }

    public ReaderPool readerPool()
    {
        return readerPool;
    }
}
