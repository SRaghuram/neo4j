/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import com.neo4j.causalclustering.core.consensus.log.EntryRecord;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.neo4j.cursor.IOCursor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * The segmented RAFT log is an append only log supporting the operations required to support
 * the RAFT consensus algorithm.
 * <p>
 * A RAFT log must be able to append new entries, but also truncate not yet committed entries,
 * prune out old compacted entries and skip to a later starting point.
 * <p>
 * The RAFT log consists of a sequence of individual log files, called segments, with
 * the following format:
 * <p>
 * [HEADER] [ENTRY]*
 * <p>
 * So a header with zero or more entries following it. Each segment file contains a consecutive
 * sequence of appended entries. The operations of truncating and skipping in the log is implemented
 * by switching to the next segment file, called the next version. A new segment file is also started
 * when the threshold for a particular file has been reached.
 */
public class SegmentedRaftLog extends LifecycleAdapter implements RaftLog
{
    private final int READER_POOL_MAX_AGE = 1; // minutes

    private final FileSystemAbstraction fileSystem;
    private final Path directory;
    private final long rotateAtSize;
    private final Function<Integer,ChannelMarshal<ReplicatedContent>> marshalSelector;
    private final FileNames fileNames;
    private final MemoryTracker memoryTracker;
    private final JobScheduler scheduler;
    private final Log log;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private boolean needsRecovery;
    private final LogProvider logProvider;
    private final SegmentedRaftLogPruner pruner;

    private State state;
    private final ReaderPool readerPool;
    private JobHandle<?> readerPoolPruner;

    public SegmentedRaftLog( FileSystemAbstraction fileSystem, Path directory, long rotateAtSize,
            Function<Integer,ChannelMarshal<ReplicatedContent>> marshalSelector, LogProvider logProvider, int readerPoolSize, Clock clock,
            JobScheduler scheduler, CoreLogPruningStrategy pruningStrategy, MemoryTracker memoryTracker )
    {
        this.fileSystem = fileSystem;
        this.directory = directory;
        this.rotateAtSize = rotateAtSize;
        this.marshalSelector = marshalSelector;
        this.logProvider = logProvider;
        this.scheduler = scheduler;

        this.fileNames = new FileNames( directory );
        this.memoryTracker = memoryTracker;
        this.readerPool = new ReaderPool( readerPoolSize, logProvider, fileNames, fileSystem, clock );
        this.pruner = new SegmentedRaftLogPruner( pruningStrategy );
        this.log = logProvider.getLog( getClass() );
        var reentrantReadWriteLock = new ReentrantReadWriteLock();
        readLock = reentrantReadWriteLock.readLock();
        writeLock = reentrantReadWriteLock.writeLock();
    }

    @Override
    public void start() throws IOException, DamagedLogStorageException, DisposedException
    {
        writeLock.lock();
        try
        {
            if ( Files.notExists( directory ) )
            {
                Files.createDirectories( directory );
            }

            try
            {
                state = new RecoveryProtocol( fileSystem, fileNames, readerPool, marshalSelector, logProvider, memoryTracker ).run();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }

            log.info( "log started with recovered state %s", state );
            /*
             * Recovery guarantees that once complete the header of the last raft log file is intact. No such guarantee
             * is made for the last log entry in the last file (or any of the files for that matter). To complete
             * recovery we need to rotate away the last log file, so that any incomplete entries at the end of the last
             * do not have entries appended after them, which would result in unaligned (and therefore wrong) reads.
             * As an obvious optimization, we don't need to rotate if the file contains only the header, such as is
             * the case of a newly created log.
             */
            SegmentFile lastSegment = state.segments().last();
            if ( lastSegment.size() > lastSegment.header().recordOffset() )
            {
                rotateSegment( state.appendIndex(), state.appendIndex(), state.terms().latest() );
            }

            readerPoolPruner = scheduler.scheduleRecurring( Group.RAFT_READER_POOL_PRUNER,
                    () -> readerPool.prune( READER_POOL_MAX_AGE, MINUTES ), READER_POOL_MAX_AGE, READER_POOL_MAX_AGE, MINUTES );
        }
        finally
        {
            writeLock.unlock();
        }
    }

    @Override
    public void stop() throws Exception
    {
        writeLock.lock();
        try
        {
            if ( readerPoolPruner != null )
            {
                readerPoolPruner.cancel();
            }
            readerPool.close();
            state.segments().close();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    @Override
    public long append( RaftLogEntry... entries ) throws IOException
    {
        writeLock.lock();
        try
        {
            ensureOk();

            try
            {
                for ( RaftLogEntry entry : entries )
                {
                    state.setAppendIndex( state.appendIndex() + 1 );
                    state.terms().append( state.appendIndex(), entry.term() );
                    state.segments().last().write( state.appendIndex(), entry );
                }
                state.segments().last().flush();
            }
            catch ( Throwable e )
            {
                needsRecovery = true;
                throw e;
            }

            if ( state.segments().last().position() >= rotateAtSize )
            {
                rotateSegment( state.appendIndex(), state.appendIndex(), state.terms().latest() );
            }

            return state.appendIndex();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private void ensureOk()
    {
        if ( needsRecovery )
        {
            throw new IllegalStateException( "Raft log requires recovery" );
        }
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        writeLock.lock();
        try
        {
            log.debug( "Truncate from index %d", fromIndex );
            if ( state.appendIndex() < fromIndex )
            {
                throw new IllegalArgumentException( "Cannot truncate at index " + fromIndex + " when append index is " +
                                                    state.appendIndex() );
            }

            long newAppendIndex = fromIndex - 1;
            long newTerm = readEntryTerm( newAppendIndex );
            truncateSegment( state.appendIndex(), newAppendIndex, newTerm );

            state.setAppendIndex( newAppendIndex );
            state.terms().truncate( fromIndex );
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private void rotateSegment( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        state.segments().last().closeWriter();
        state.segments().rotate( prevFileLastIndex, prevIndex, prevTerm );
    }

    private void truncateSegment( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        state.segments().last().closeWriter();
        state.segments().truncate( prevFileLastIndex, prevIndex, prevTerm );
    }

    private void skipSegment( long prevFileLastIndex, long prevIndex, long prevTerm ) throws IOException
    {
        state.segments().last().closeWriter();
        state.segments().skip( prevFileLastIndex, prevIndex, prevTerm );
    }

    @Override
    public long appendIndex()
    {
        readLock.lock();
        try
        {
            return state.appendIndex();
        }
        finally
        {
            readLock.unlock();
        }
    }

    @Override
    public long prevIndex()
    {
        readLock.lock();
        try
        {
            return state.prevIndex();
        }
        finally
        {
            readLock.unlock();
        }
    }

    @Override
    public RaftLogCursor getEntryCursor( long fromIndex )
    {
        final IOCursor<EntryRecord> inner = new EntryCursor( state.segments(), fromIndex );
        return new SegmentedRaftLogCursor( fromIndex, inner );
    }

    @Override
    public long skip( long newIndex, long newTerm ) throws IOException
    {
        writeLock.lock();
        try
        {
            log.info( "Skipping from {index: %d, term: %d} to {index: %d, term: %d}",
                    state.appendIndex(), state.terms().latest(), newIndex, newTerm );

            if ( state.appendIndex() < newIndex )
            {
                skipSegment( state.appendIndex(), newIndex, newTerm );
                state.terms().skip( newIndex, newTerm );

                state.setPrevIndex( newIndex );
                state.setPrevTerm( newTerm );
                state.setAppendIndex( newIndex );
            }

            return state.appendIndex();
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private RaftLogEntry readLogEntry( long logIndex ) throws IOException
    {
        try ( IOCursor<EntryRecord> cursor = new EntryCursor( state.segments(), logIndex ) )
        {
            return cursor.next() ? cursor.get().logEntry() : null;
        }
    }

    @Override
    public long readEntryTerm( long logIndex ) throws IOException
    {
        readLock.lock();
        try
        {
            if ( logIndex > state.appendIndex() )
            {
                return -1;
            }
            long term = state.terms().get( logIndex );
            if ( term != -1 || logIndex < state.prevIndex() )
            {
                return term;
            }
            RaftLogEntry entry = readLogEntry( logIndex );
            term = (entry != null) ? entry.term() : -1;
            return term;
        }
        finally
        {
            readLock.unlock();
        }
    }

    @Override
    public long prune( long safeIndex )
    {
        writeLock.lock();
        try
        {
            log.debug( "Prune to %s. Current state is %s", safeIndex, state );
            long pruneIndex = pruner.getIndexToPruneFrom( safeIndex, state.segments() );
            SegmentFile oldestNotDisposed = state.segments().prune( pruneIndex );

            long newPrevIndex = oldestNotDisposed.header().prevIndex();
            long newPrevTerm = oldestNotDisposed.header().prevTerm();

            if ( newPrevIndex > state.prevIndex() )
            {
                state.setPrevIndex( newPrevIndex );
            }

            if ( newPrevTerm > state.prevTerm() )
            {
                state.setPrevTerm( newPrevTerm );
            }

            log.debug( "Updated state is now %s", state );

            state.terms().prune( state.prevIndex() );

            return state.prevIndex();
        }
        finally
        {
            writeLock.unlock();
        }
    }
}
