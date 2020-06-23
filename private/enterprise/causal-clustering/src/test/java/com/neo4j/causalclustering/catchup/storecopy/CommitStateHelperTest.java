/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.Files.exists;
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

    private CommitStateHelper commitStateHelper;
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUp()
    {
        File txLogLocation = new File( testDirectory.homeDir(), "txLogLocation" );
        var config = Config.newBuilder()
                .set( GraphDatabaseSettings.neo4j_home, testDirectory.homeDir().toPath() )
                .set( GraphDatabaseSettings.transaction_logs_root_path, txLogLocation.toPath().toAbsolutePath() )
                .build();
        databaseLayout = DatabaseLayout.of( config );
        commitStateHelper = new CommitStateHelper( pageCache, fsa, config, selectStorageEngine() );
    }

    @Test
    void shouldNotHaveTxLogsIfDirectoryDoesNotExist() throws IOException
    {
        Path txDir = databaseLayout.getTransactionLogsDirectory();
        assertFalse( exists( txDir ) );
        assertFalse( commitStateHelper.hasTxLogs( databaseLayout ) );
    }

    @Test
    void shouldNotHaveTxLogsIfDirectoryIsEmpty() throws IOException
    {
        Path txDir = databaseLayout.getTransactionLogsDirectory();
        fsa.mkdir( txDir.toFile() );

        assertFalse( commitStateHelper.hasTxLogs( databaseLayout ) );
    }

    @Test
    void shouldNotHaveTxLogsIfDirectoryHasFilesWithIncorrectName() throws IOException
    {
        File txDir = databaseLayout.getTransactionLogsDirectory().toFile();
        fsa.mkdirs( txDir );

        fsa.write( new File( txDir, "foo.bar" ) ).close();

        assertFalse( commitStateHelper.hasTxLogs( databaseLayout ) );
    }

    @Test
    void shouldHaveTxLogsIfDirectoryHasTxFile() throws IOException
    {
        File txDir = databaseLayout.getTransactionLogsDirectory().toFile();
        fsa.mkdirs( txDir );
        fsa.write( new File( txDir, TransactionLogFilesHelper.DEFAULT_NAME + ".0" ) ).close();

        assertTrue( commitStateHelper.hasTxLogs( databaseLayout ) );
    }
}
