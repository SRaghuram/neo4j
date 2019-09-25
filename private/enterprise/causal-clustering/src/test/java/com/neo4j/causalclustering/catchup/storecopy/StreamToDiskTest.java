/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.helpers.Buffers;
import io.netty.buffer.ByteBuf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class StreamToDiskTest
{
    @Rule
    public final Buffers buffers = new Buffers();

    private static final byte[] DATA = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( fs ).around( directory ).around( pageCacheRule );

    @Test
    public void shouldLetPageCacheHandleRecordStoresAndNativeLabelScanStoreFiles() throws Exception
    {
        DatabaseLayout layout = DatabaseLayout.ofFlat( directory.file( DEFAULT_DATABASE_NAME ) );
        // GIVEN
        Monitors monitors = new Monitors();
        StreamToDiskProvider writerProvider = new StreamToDiskProvider( layout.databaseDirectory(), fs, monitors );

        // WHEN
        for ( StoreType type : StoreType.values() )
        {
            File file = layout.file( type.getDatabaseFile() );
            writeAndVerify( writerProvider, file );
        }
        writeAndVerify( writerProvider, layout.labelScanStore() );
    }

    private void writeAndVerify( StreamToDiskProvider writerProvider, File file ) throws Exception
    {
        try ( StoreFileStream acquire = writerProvider.acquire( file.getName(), 16 ) )
        {
            ByteBuf buffer = buffers.buffer();
            buffer.writeBytes( DATA );
            acquire.write( buffer );
        }
        assertTrue( "Streamed file created.", fs.fileExists( file ) );
        assertEquals( DATA.length, fs.getFileSize( file ) );
    }
}
