/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.fail;

public class TestConfiguration
{
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    public TestDirectory dir = TestDirectory.testDirectory();
    @Rule
    public RuleChain rules = RuleChain.outerRule( dir ).around( suppressOutput );

    private static final String HOST_ADDRESS = "127.0.0.1";

    private File storeDir;
    private String backupDir;

    @Before
    public void before() throws Exception
    {
        storeDir = dir.databaseDir();
        backupDir = dir.cleanDirectory( "full-backup" ).getAbsolutePath();
    }

    @Test
    public void testOnByDefault()
    {
        int port = PortAuthority.allocatePort();

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + port ).newGraphDatabase();
        OnlineBackup.from( HOST_ADDRESS, port ).full( backupDir );
        db.shutdown();
    }

    @Test
    public void testOffByConfig()
    {
        int port = PortAuthority.allocatePort();

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + port )
                .newGraphDatabase();
        try
        {
            OnlineBackup.from( HOST_ADDRESS, port ).full( backupDir );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }
        db.shutdown();
    }

    @Test
    public void testEnableDefaultsInConfig()
    {
        int port = PortAuthority.allocatePort();

        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, "localhost:" + port )
                .newGraphDatabase();

        OnlineBackup.from( HOST_ADDRESS, port ).full( backupDir );
        db.shutdown();
    }

    @Test
    public void testEnableCustomPortInConfig()
    {
        int customPort = PortAuthority.allocatePort();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE )
                .setConfig( OnlineBackupSettings.online_backup_server, ":" + customPort )
                .newGraphDatabase();
        try
        {
            OnlineBackup.from( HOST_ADDRESS, PortAuthority.allocatePort() ).full( backupDir );
            fail( "Shouldn't be possible" );
        }
        catch ( Exception e )
        { // Good
        }

        OnlineBackup.from( HOST_ADDRESS, customPort ).full( backupDir );
        db.shutdown();
    }
}
