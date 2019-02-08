/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.LayoutConfig;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.storageengine.api.StorageEngineFactory.selectStorageEngine;

@PageCacheExtension
class CommitStateHelperTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fsa;
    @Inject
    private PageCache pageCache;

    private Config config;
    private CommitStateHelper commitStateHelper;
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUp()
    {
        File txLogLocation = new File( testDirectory.directory(), "txLogLocation" );
        config = Config.builder().withSetting( GraphDatabaseSettings.transaction_logs_root_path,
                txLogLocation.getAbsolutePath() ).build();
        File storeDir = testDirectory.storeDir();
        databaseLayout = DatabaseLayout.of( storeDir, LayoutConfig.of( config ), config.get( GraphDatabaseSettings.active_database ) );
        commitStateHelper = new CommitStateHelper( pageCache, fsa, config, selectStorageEngine( Service.load( StorageEngineFactory.class ) ) );
    }

    @Test
    void shouldNotHaveTxLogsIfDirectoryDoesNotExist() throws IOException
    {
        File txDir = databaseLayout.getTransactionLogsDirectory();
        assertFalse( txDir.exists() );
        assertFalse( commitStateHelper.hasTxLogs( databaseLayout ) );
    }

    @Test
    void shouldNotHaveTxLogsIfDirectoryIsEmpty() throws IOException
    {
        File txDir = databaseLayout.getTransactionLogsDirectory();
        fsa.mkdir( txDir );

        assertFalse( commitStateHelper.hasTxLogs( databaseLayout ) );
    }

    @Test
    void shouldNotHaveTxLogsIfDirectoryHasFilesWithIncorrectName() throws IOException
    {
        File txDir = databaseLayout.getTransactionLogsDirectory();
        fsa.mkdirs( txDir );

        fsa.create( new File( txDir, "foo.bar" ) ).close();

        assertFalse( commitStateHelper.hasTxLogs( databaseLayout ) );
    }

    @Test
    void shouldHaveTxLogsIfDirectoryHasTxFile() throws IOException
    {
        File txDir = databaseLayout.getTransactionLogsDirectory();
        fsa.mkdirs( txDir );
        fsa.create( new File( txDir, TransactionLogFiles.DEFAULT_NAME + ".0" ) ).close();

        assertTrue( commitStateHelper.hasTxLogs( databaseLayout ) );
    }
}
