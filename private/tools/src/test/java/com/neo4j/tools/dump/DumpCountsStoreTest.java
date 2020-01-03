/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.counts.CountsBuilder.EMPTY;
import static org.neo4j.internal.counts.CountsKey.nodeKey;
import static org.neo4j.internal.counts.CountsKey.relationshipKey;
import static org.neo4j.internal.counts.GBPTreeCountsStore.NO_MONITOR;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

@PageCacheExtension
@ExtendWith( SuppressOutputExtension.class )
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
        File file = directory.file( "file" );
        try ( GBPTreeCountsStore store = new GBPTreeCountsStore( pageCache, file, immediate(), EMPTY, false, NO_MONITOR ) )
        {
            store.start();
            try ( CountsAccessor.Updater updater = store.apply( BASE_TX_ID + 1 ) )
            {
                updater.incrementNodeCount( 0, 4 );
                updater.incrementNodeCount( 1, 5 );
                updater.incrementNodeCount( -1, 9 );
                updater.incrementRelationshipCount( 0, 4, 1, 67 );
            }
            store.checkpoint( UNLIMITED );
        }

        // when
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream outStream = new PrintStream( out );
        DumpCountsStore.main( new String[]{file.getAbsolutePath()}, outStream );
        outStream.close();

        // then
        String output = out.toString();
        assertThat( output, containsString( nodeKey( 0 ) + " = 4" ) );
        assertThat( output, containsString( nodeKey( 1 ) + " = 5" ) );
        assertThat( output, containsString( nodeKey( -1 ) + " = 9" ) );
        assertThat( output, containsString( relationshipKey( 0, 4, 1 ) + " = 67" ) );
    }
}
