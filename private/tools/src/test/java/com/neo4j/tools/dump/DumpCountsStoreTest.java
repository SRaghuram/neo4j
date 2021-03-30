/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.counts.CountsBuilder.EMPTY;
import static org.neo4j.internal.counts.GBPTreeCountsStore.NO_MONITOR;
import static org.neo4j.internal.counts.GBPTreeCountsStore.keyToString;
import static org.neo4j.internal.counts.GBPTreeCountsStore.nodeKey;
import static org.neo4j.internal.counts.GBPTreeCountsStore.relationshipKey;
import static org.neo4j.io.pagecache.IOController.DISABLED;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@PageCacheExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class DumpCountsStoreTest
{
    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    @Test
    void shouldDumpCountsStore() throws Exception
    {
        // given
        Path file = directory.file( "file" );
        try ( GBPTreeCountsStore store = new GBPTreeCountsStore( pageCache, file, directory.getFileSystem(), immediate(), EMPTY, false, PageCacheTracer.NULL,
                NO_MONITOR, DEFAULT_DATABASE_NAME ) )
        {
            store.start( NULL, INSTANCE );
            try ( CountsAccessor.Updater updater = store.apply( BASE_TX_ID + 1, NULL ) )
            {
                updater.incrementNodeCount( 0, 4 );
                updater.incrementNodeCount( 1, 5 );
                updater.incrementNodeCount( -1, 9 );
                updater.incrementRelationshipCount( 0, 4, 1, 67 );
            }
            store.checkpoint( DISABLED, NULL );
        }

        // when
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream( out );
        DumpCountsStore.main( new String[]{file.toAbsolutePath().toString()}, outStream );
        outStream.close();

        // then
        String output = out.toString();
        assertThat( output ).contains( keyToString( nodeKey( 0 ) ) + " = 4" );
        assertThat( output ).contains( keyToString( nodeKey( 1 ) ) + " = 5" );
        assertThat( output ).contains( keyToString( nodeKey( -1 ) ) + " = 9" );
        assertThat( output ).contains( keyToString( relationshipKey( 0, 4, 1 ) ) + " = 67" );
    }
}
