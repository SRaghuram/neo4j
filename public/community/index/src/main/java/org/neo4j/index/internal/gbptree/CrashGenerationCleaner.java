/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.helpers.Exceptions;
import org.neo4j.index.internal.gbptree.GBPTree.Monitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Scans the entire tree and checks all GSPPs, replacing all CRASH gen GSPs with zeros.
 */
class CrashGenerationCleaner
{
    private static final long MIN_BATCH_SIZE = 10;
    static final long MAX_BATCH_SIZE = 1000;
    private final PagedFile pagedFile;
    private final TreeNode<?,?> treeNode;
    private final long lowTreeNodeId;
    private final long highTreeNodeId;
    private final long stableGeneration;
    private final long unstableGeneration;
    private final Monitor monitor;

    CrashGenerationCleaner( PagedFile pagedFile, TreeNode<?,?> treeNode, long lowTreeNodeId, long highTreeNodeId,
            long stableGeneration, long unstableGeneration, Monitor monitor )
    {
        this.pagedFile = pagedFile;
        this.treeNode = treeNode;
        this.lowTreeNodeId = lowTreeNodeId;
        this.highTreeNodeId = highTreeNodeId;
        this.stableGeneration = stableGeneration;
        this.unstableGeneration = unstableGeneration;
        this.monitor = monitor;
    }

    private static long batchSize( long pagesToClean, int threads )
    {
        // Batch size at most maxBatchSize, at least minBatchSize and trying to give each thread 100 batches each
        return min( MAX_BATCH_SIZE, max( MIN_BATCH_SIZE, pagesToClean / (100 * threads) ) );
    }

    // === Methods about the execution and threading ===

    public void clean( ExecutorService executor )
    {
        monitor.cleanupStarted();
        assert unstableGeneration > stableGeneration : unexpectedGenerations();
        assert unstableGeneration - stableGeneration > 1 : unexpectedGenerations();

        long startTime = currentTimeMillis();
        long pagesToClean = highTreeNodeId - lowTreeNodeId;
        int threads = Runtime.getRuntime().availableProcessors();
        long batchSize = batchSize( pagesToClean, threads );
        AtomicLong nextId = new AtomicLong( lowTreeNodeId );
        AtomicReference<Throwable> error = new AtomicReference<>();
        LongAdder cleanedPointers = new LongAdder();
        LongAdder numberOfTreeNodes = new LongAdder();
        CountDownLatch activeThreadLatch = new CountDownLatch( threads );
        for ( int i = 0; i < threads; i++ )
        {
            executor.submit( cleaner( nextId, batchSize, numberOfTreeNodes, cleanedPointers, activeThreadLatch, error ) );
        }

        try
        {
            long lastProgression = nextId.get();
            // Have max no-progress-timeout quite high to be able to cope with huge
            // I/O congestion spikes w/o failing in vain.
            while ( !activeThreadLatch.await( 30, SECONDS ) )
            {
                if ( lastProgression == nextId.get() )
                {
                    // No progression at all, abort
                    error.compareAndSet( null, new IOException( "No progress, so forcing abort" ) );
                }
                lastProgression = nextId.get();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }

        Throwable finalError = error.get();
        if ( finalError != null )
        {
            Exceptions.throwIfUnchecked( finalError );
            throw new RuntimeException( finalError );
        }

        long endTime = currentTimeMillis();
        monitor.cleanupFinished( pagesToClean, numberOfTreeNodes.sum(), cleanedPointers.sum(), endTime - startTime );
    }

    private Runnable cleaner( AtomicLong nextId, long batchSize, LongAdder numberOfTreeNodes, LongAdder cleanedPointers,
            CountDownLatch activeThreadLatch, AtomicReference<Throwable> error )
    {
        return () ->
        {
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK );
                    PageCursor writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                long localNextId;
                while ( ( localNextId = nextId.getAndAdd( batchSize )) < highTreeNodeId )
                {
                    int localNumberOfTreeNodes = 0;
                    for ( int i = 0; i < batchSize && localNextId < highTreeNodeId; i++, localNextId++ )
                    {
                        PageCursorUtil.goTo( cursor, "clean", localNextId );

                        boolean isTreeNode = isTreeNode( cursor );
                        if ( isTreeNode )
                        {
                            localNumberOfTreeNodes++;
                            if ( hasCrashedGSPP( this.treeNode, cursor ) )
                            {
                                writeCursor.next( cursor.getCurrentPageId() );
                                cleanTreeNode( this.treeNode, writeCursor, cleanedPointers );
                            }
                        }
                    }
                    numberOfTreeNodes.add( localNumberOfTreeNodes );

                    // Check error status after a batch, to reduce volatility overhead.
                    // Is this over thinking things? Perhaps
                    if ( error.get() != null )
                    {
                        break;
                    }
                }
            }
            catch ( Throwable e )
            {
                error.accumulateAndGet( e, Exceptions::chain );
            }
            finally
            {
                activeThreadLatch.countDown();
            }
        };
    }

    // === Methods about checking if a tree node has crashed pointers ===

    private boolean isTreeNode( PageCursor cursor ) throws IOException
    {
        boolean isTreeNode;
        do
        {
            isTreeNode = TreeNode.nodeType( cursor ) == TreeNode.NODE_TYPE_TREE_NODE;
        }
        while ( cursor.shouldRetry() );
        PageCursorUtil.checkOutOfBounds( cursor );
        return isTreeNode;
    }

    private boolean hasCrashedGSPP( TreeNode<?,?> treeNode, PageCursor cursor ) throws IOException
    {
        int keyCount;
        do
        {
            keyCount = TreeNode.keyCount( cursor );
        }
        while ( cursor.shouldRetry() );
        PageCursorUtil.checkOutOfBounds( cursor );

        boolean hasCrashed;
        do
        {
            hasCrashed =
                    hasCrashedGSPP( cursor, TreeNode.BYTE_POS_SUCCESSOR ) ||
                    hasCrashedGSPP( cursor, TreeNode.BYTE_POS_LEFTSIBLING ) ||
                    hasCrashedGSPP( cursor, TreeNode.BYTE_POS_RIGHTSIBLING );

            if ( !hasCrashed && TreeNode.isInternal( cursor ) )
            {
                for ( int i = 0; i <= keyCount && treeNode.reasonableChildCount( i ) && !hasCrashed; i++ )
                {
                    hasCrashed = hasCrashedGSPP( cursor, treeNode.childOffset( i ) );
                }
            }
        }
        while ( cursor.shouldRetry() );
        PageCursorUtil.checkOutOfBounds( cursor );
        return hasCrashed;
    }

    private boolean hasCrashedGSPP( PageCursor cursor, int gsppOffset )
    {
        return hasCrashedGSP( cursor, gsppOffset ) || hasCrashedGSP( cursor, gsppOffset + GenerationSafePointer.SIZE );
    }

    private boolean hasCrashedGSP( PageCursor cursor, int offset )
    {
        cursor.setOffset( offset );
        long generation = GenerationSafePointer.readGeneration( cursor );
        return generation > stableGeneration && generation < unstableGeneration;
    }

    // === Methods about actually cleaning a discovered crashed tree node ===

    private void cleanTreeNode( TreeNode<?,?> treeNode, PageCursor cursor, LongAdder cleanedPointers )
    {
        cleanCrashedGSPP( cursor, TreeNode.BYTE_POS_SUCCESSOR, cleanedPointers );
        cleanCrashedGSPP( cursor, TreeNode.BYTE_POS_LEFTSIBLING, cleanedPointers );
        cleanCrashedGSPP( cursor, TreeNode.BYTE_POS_RIGHTSIBLING, cleanedPointers );

        if ( TreeNode.isInternal( cursor ) )
        {
            int keyCount = TreeNode.keyCount( cursor );
            for ( int i = 0; i <= keyCount && treeNode.reasonableChildCount( i ); i++ )
            {
                cleanCrashedGSPP( cursor, treeNode.childOffset( i ), cleanedPointers );
            }
        }
    }

    private void cleanCrashedGSPP( PageCursor cursor, int gsppOffset, LongAdder cleanedPointers )
    {
        cleanCrashedGSP( cursor, gsppOffset, cleanedPointers );
        cleanCrashedGSP( cursor, gsppOffset + GenerationSafePointer.SIZE, cleanedPointers );
    }

    /**
     * NOTE: No shouldRetry is used because cursor is assumed to be a write cursor.
     */
    private void cleanCrashedGSP( PageCursor cursor, int gspOffset, LongAdder cleanedPointers )
    {
        if ( hasCrashedGSP( cursor, gspOffset ) )
        {
            cursor.setOffset( gspOffset );
            GenerationSafePointer.clean( cursor );
            cleanedPointers.increment();
        }
    }

    private String unexpectedGenerations( )
    {
        return "Unexpected generations, stableGeneration=" + stableGeneration + ", unstableGeneration=" + unstableGeneration;
    }
}
