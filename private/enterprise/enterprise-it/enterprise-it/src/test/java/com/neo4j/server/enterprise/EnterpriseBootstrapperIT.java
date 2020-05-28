/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.BaseBootstrapperIT;
import org.neo4j.server.NeoBootstrapper;
import org.neo4j.test.rule.CleanupRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.logs_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_level;
import static org.neo4j.internal.helpers.collection.MapUtil.store;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.WebContainerTestUtils.getDefaultRelativeProperties;
import static org.neo4j.server.WebContainerTestUtils.getRelativePath;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

public class EnterpriseBootstrapperIT extends BaseBootstrapperIT
{
    @Rule
    public final CleanupRule cleanupRule = new CleanupRule();

    @Override
    protected NeoBootstrapper newBootstrapper()
    {
        return new UncoveredEnterpriseBootstrapper();
    }

    @Override
    protected String[] getAdditionalArguments()
    {
        String[] args = new String[]{"-c", OnlineBackupSettings.online_backup_enabled.name() + "=false"};
        return ArrayUtils.addAll( super.getAdditionalArguments(), args );
    }

    @Override
    protected DatabaseManagementService newEmbeddedDbms( File homeDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( homeDir ).build();
    }

    @Test
    public void shouldBeAbleToStartInSingleMode() throws Exception
    {
        // When
        int resultCode = NeoBootstrapper.start( bootstrapper, withConnectorsOnRandomPortsConfig(
                "--home-dir", testDirectory.directory( "home-dir" ).getAbsolutePath(),
                "-c", configOption( GraphDatabaseSettings.mode, "SINGLE" ),
                "-c", configOption( data_directory, getRelativePath( folder.homeDir(), data_directory ).toString() ),
                "-c", configOption( logs_directory, testDirectory.homeDir().getAbsolutePath() ),
                "-c", "dbms.connector.bolt.listen_address=:0" ) );

        // Then
        assertEquals( NeoBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, TRUE, 1, TimeUnit.MINUTES );
    }

    @Test
    public void debugLoggingDisabledByDefault() throws Exception
    {
        // When
        File configFile = testDirectory.file( Config.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap();
        properties.putAll( getDefaultRelativeProperties( testDirectory.homeDir() ) );
        properties.putAll( connectorsOnRandomPortsConfig() );
        store( properties, configFile );

        // When
        UncoveredEnterpriseBootstrapper uncoveredEnterpriseBootstrapper = new UncoveredEnterpriseBootstrapper();
        cleanupRule.add( uncoveredEnterpriseBootstrapper );
        NeoBootstrapper.start( uncoveredEnterpriseBootstrapper,
                "--home-dir", testDirectory.directory( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath() );

        // Then
        assertEventually( "Server was started", uncoveredEnterpriseBootstrapper::isRunning, TRUE, 1, TimeUnit.MINUTES );
        LogProvider userLogProvider = uncoveredEnterpriseBootstrapper.getUserLogProvider();
        assertFalse( "Debug logging is disabled by default", userLogProvider.getLog( getClass() ).isDebugEnabled() );
    }

    @Test
    public void debugLoggingEnabledBySetting() throws Exception
    {
        // When
        File configFile = testDirectory.file( Config.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap( store_internal_log_level.name(), "DEBUG");
        properties.putAll( getDefaultRelativeProperties( testDirectory.homeDir() ) );
        properties.putAll( connectorsOnRandomPortsConfig() );
        store( properties, configFile );

        // When
        UncoveredEnterpriseBootstrapper uncoveredEnterpriseBootstrapper = new UncoveredEnterpriseBootstrapper();
        cleanupRule.add( uncoveredEnterpriseBootstrapper );
        NeoBootstrapper.start( uncoveredEnterpriseBootstrapper,
                "--home-dir", testDirectory.directory( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath() );

        // Then
        assertEventually( "Server was started", uncoveredEnterpriseBootstrapper::isRunning, TRUE, 1, TimeUnit.MINUTES );
        LogProvider userLogProvider = uncoveredEnterpriseBootstrapper.getUserLogProvider();
        assertTrue( "Debug logging enabled by setting value.", userLogProvider.getLog( getClass() ).isDebugEnabled() );
    }

    private static class UncoveredEnterpriseBootstrapper extends EnterpriseBootstrapper
    {
        private LogProvider userLogProvider;

        @Override
        protected DatabaseManagementService createNeo( Config config, GraphDatabaseDependencies dependencies )
        {
            this.userLogProvider = dependencies.userLogProvider();
            return super.createNeo( buildTestConfig( config ), dependencies );
        }

        LogProvider getUserLogProvider()
        {
            return userLogProvider;
        }

        private static Config buildTestConfig( Config fromConfig )
        {
            return Config.newBuilder().fromConfig( fromConfig )
                    .set( GraphDatabaseSettings.pagecache_memory, "8m" )
                    .set( OnlineBackupSettings.online_backup_listen_address, new SocketAddress( "127.0.0.1", 0 ) )
                    .set( OnlineBackupSettings.online_backup_enabled, false )
                    .set( GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.kibiBytes( 128 ) )
                    .build();
        }
    }
}
