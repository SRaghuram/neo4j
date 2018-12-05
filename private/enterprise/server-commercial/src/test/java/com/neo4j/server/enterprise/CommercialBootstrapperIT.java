/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.commercial.edition.CommercialGraphDatabase;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.BaseBootstrapperIT;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBootstrapper;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.test.rule.CleanupRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_level;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig.certificates_directory;
import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CommercialBootstrapperIT extends BaseBootstrapperIT
{
    private final TemporaryFolder folder = new TemporaryFolder();
    private final CleanupRule cleanupRule = new CleanupRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(folder).around( cleanupRule );

    @Override
    protected ServerBootstrapper newBootstrapper()
    {
        return new CommercialBootstrapper();
    }

    @Test
    public void shouldBeAbleToStartInSingleMode() throws Exception
    {
        // When
        int resultCode = ServerBootstrapper.start( bootstrapper, withConnectorsOnRandomPortsConfig(
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "-c", configOption( EnterpriseEditionSettings.mode, "SINGLE" ),
                "-c", configOption( data_directory, getRelativePath( folder.getRoot(), data_directory ) ),
                "-c", configOption( logs_directory, tempDir.getRoot().getAbsolutePath() ),
                "-c", configOption( certificates_directory, getRelativePath( folder.getRoot(), certificates_directory ) ) ) );

        // Then
        assertEquals( ServerBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
        assertThat( bootstrapper.getServer().getDatabase().getGraph(), instanceOf( CommercialGraphDatabase.class ) );
    }

    @Test
    public void debugLoggingDisabledByDefault() throws Exception
    {
        // When
        File configFile = tempDir.newFile( Config.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap();
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.putAll( connectorsOnRandomPortsConfig() );
        store( properties, configFile );

        // When
        UncoveredCommercialBootstrapper uncoveredEnterpriseBootstrapper = new UncoveredCommercialBootstrapper();
        cleanupRule.add( uncoveredEnterpriseBootstrapper );
        ServerBootstrapper.start( uncoveredEnterpriseBootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath() );

        // Then
        assertEventually( "Server was started", uncoveredEnterpriseBootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
        LogProvider userLogProvider = uncoveredEnterpriseBootstrapper.getUserLogProvider();
        assertFalse( "Debug logging is disabled by default", userLogProvider.getLog( getClass() ).isDebugEnabled() );
    }

    @Test
    public void debugLoggingEnabledBySetting() throws Exception
    {
        // When
        File configFile = tempDir.newFile( Config.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap( store_internal_log_level.name(), "DEBUG");
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.putAll( connectorsOnRandomPortsConfig() );
        store( properties, configFile );

        // When
        UncoveredCommercialBootstrapper uncoveredEnterpriseBootstrapper = new UncoveredCommercialBootstrapper();
        cleanupRule.add( uncoveredEnterpriseBootstrapper );
        ServerBootstrapper.start( uncoveredEnterpriseBootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath() );

        // Then
        assertEventually( "Server was started", uncoveredEnterpriseBootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
        LogProvider userLogProvider = uncoveredEnterpriseBootstrapper.getUserLogProvider();
        assertTrue( "Debug logging enabled by setting value.", userLogProvider.getLog( getClass() ).isDebugEnabled() );
    }

    private static class UncoveredCommercialBootstrapper extends CommercialBootstrapper
    {
        private LogProvider userLogProvider;

        @Override
        protected NeoServer createNeoServer( GraphFactory graphFactory, Config config, GraphDatabaseDependencies dependencies )
        {
            this.userLogProvider = dependencies.userLogProvider();
            return super.createNeoServer( graphFactory, config, dependencies );
        }

        LogProvider getUserLogProvider()
        {
            return userLogProvider;
        }
    }
}
