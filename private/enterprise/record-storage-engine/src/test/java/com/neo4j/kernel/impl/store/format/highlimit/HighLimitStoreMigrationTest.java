/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.common.ProgressReporter;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.storemigration.RecordStorageMigrator;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;

@PageCacheExtension
class HighLimitStoreMigrationTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    @Test
    void haveDifferentFormatCapabilitiesAsHighLimit3_0()
    {
        assertFalse( HighLimit.RECORD_FORMATS.hasCompatibleCapabilities( HighLimitV3_0_0.RECORD_FORMATS, CapabilityType.FORMAT ) );
    }

    @Test
    void haveSameFormatCapabilitiesAsHighLimit3_4()
    {
        assertTrue( HighLimit.RECORD_FORMATS.hasCompatibleCapabilities( HighLimitV3_4_0.RECORD_FORMATS, CapabilityType.FORMAT ) );
    }

    @Test
    void migrateHighLimit3_0StoreFiles() throws Exception
    {
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler() )
        {
            RecordStorageMigrator migrator = new RecordStorageMigrator( fileSystem, pageCache, Config.defaults(), NullLogService.getInstance(), jobScheduler );

            DatabaseLayout databaseLayout = testDirectory.databaseLayout();
            DatabaseLayout migrationLayout = testDirectory.databaseLayout( "migration" );

            prepareNeoStoreFile( fileSystem, databaseLayout, HighLimitV3_0_0.STORE_VERSION, pageCache );

            ProgressReporter progressMonitor = mock( ProgressReporter.class );

            migrator.migrate( databaseLayout, migrationLayout, progressMonitor, HighLimitV3_0_0.STORE_VERSION, HighLimit.STORE_VERSION );

            int newStoreFilesCount = fileSystem.listFiles( migrationLayout.databaseDirectory() ).length;
            assertThat( "Store should be migrated and new store files should be created.", newStoreFilesCount,
                    Matchers.greaterThanOrEqualTo( StoreType.values().length ) );
        }
    }

    private static void prepareNeoStoreFile( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, String storeVersion, PageCache pageCache )
            throws IOException
    {
        File neoStoreFile = createNeoStoreFile( fileSystem, databaseLayout );
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
    }

    private static File createNeoStoreFile( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        File neoStoreFile = databaseLayout.metadataStore();
        fileSystem.create( neoStoreFile ).close();
        return neoStoreFile;
    }
}
