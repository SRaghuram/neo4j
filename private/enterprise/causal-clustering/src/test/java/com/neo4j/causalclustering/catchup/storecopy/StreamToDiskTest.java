/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.test_helpers.Buffers;
import com.neo4j.causalclustering.test_helpers.BuffersExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectorySupportExtension.class, BuffersExtension.class } )
class StreamToDiskTest
{
    @Inject
    private Buffers buffers;

    private static final byte[] DATA = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;

    @Test
    public void shouldLetPageCacheHandleRecordStoresAndNativeLabelScanStoreFiles() throws Exception
    {
        var layout = DatabaseLayout.ofFlat( directory.file( DEFAULT_DATABASE_NAME ) );
        // GIVEN
        var monitors = new Monitors();
        var writerProvider = new StreamToDiskProvider( layout.databaseDirectory(), fs, monitors );

        // WHEN
        for ( var type : StoreType.values() )
        {
            var file = layout.file( type.getDatabaseFile() );
            writeAndVerify( writerProvider, file );
        }
        writeAndVerify( writerProvider, layout.labelScanStore() );
    }

    private void writeAndVerify( StreamToDiskProvider writerProvider, Path file ) throws Exception
    {
        try ( var acquire = writerProvider.acquire( file.getFileName().toString(), 16 ) )
        {
            var buffer = buffers.buffer();
            buffer.writeBytes( DATA );
            acquire.write( buffer );
        }
        assertTrue( fs.fileExists( file ), "Streamed file created." );
        assertEquals( DATA.length, fs.getFileSize( file ) );
    }
}
