/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommitStateHelperTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private Config config;
    private CommitStateHelper commitStateHelper;
    private DatabaseLayout databaseLayout;
    private FileSystemAbstraction fsa;

    @Before
    public void setUp()
    {
        File txLogLocation = new File( testDirectory.directory(), "txLogLocation" );
        config = Config.builder().withSetting( GraphDatabaseSettings.logical_logs_location, txLogLocation.getAbsolutePath() ).build();
        File storeDir = testDirectory.storeDir();
        databaseLayout = DatabaseLayout.of( storeDir, config.get( GraphDatabaseSettings.active_database ) );
        fsa = testDirectory.getFileSystem();
        commitStateHelper = new CommitStateHelper( pageCacheRule.getPageCache( fsa ), fsa, config );
    }

    @Test
    public void shouldNotHaveTxLogsIfDirectoryDoesNotExist() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        assertFalse( txDir.exists() );
        assertFalse( commitStateHelper.hasTxLogs( databaseLayout ) );
    }

    @Test
    public void shouldNotHaveTxLogsIfDirectoryIsEmpty() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        fsa.mkdir( txDir );

        assertFalse( commitStateHelper.hasTxLogs( databaseLayout ) );
    }

    @Test
    public void shouldNotHaveTxLogsIfDirectoryHasFilesWithIncorrectName() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        fsa.mkdir( txDir );

        fsa.create( new File( txDir, "foo.bar" ) ).close();

        assertFalse( commitStateHelper.hasTxLogs( databaseLayout ) );
    }

    @Test
    public void shouldHaveTxLogsIfDirectoryHasTxFile() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        fsa.mkdir( txDir );
        fsa.create( new File( txDir, TransactionLogFiles.DEFAULT_NAME + ".0" ) ).close();

        assertTrue( commitStateHelper.hasTxLogs( databaseLayout ) );
    }
}
