/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.LoadBalancingPluginGroup;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.plugins.ServerShufflingProcessor;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerPoliciesPlugin;
import com.neo4j.configuration.CausalClusteringSettings;
import org.junit.jupiter.api.Test;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

public class LoadBalancingPluginLoaderTest
{
    public static final class DummyPlugin extends LoadBalancingPluginGroup
    {
        public final Setting<Boolean> value = getBuilder( BOOL, true ).build();

        public static DummyPlugin group( String name )
        {
            return new DummyPlugin( name );
        }

        private DummyPlugin( String name )
        {
            super( name, DUMMY_PLUGIN_NAME );
        }
    }

    private static final String DUMMY_PLUGIN_NAME = "dummy";
    private static final String DOES_NOT_EXIST = "does_not_exist";
    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @Test
    void shouldReturnSelectedPlugin() throws Throwable
    {
        // given
        Config config = Config.newBuilder()
                .set( CausalClusteringSettings.load_balancing_plugin, DUMMY_PLUGIN_NAME )
                .set( CausalClusteringSettings.load_balancing_shuffle, false ).build();

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderService.class ), NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof DummyLoadBalancingPlugin );
        assertEquals( DUMMY_PLUGIN_NAME, ((DummyLoadBalancingPlugin) plugin).pluginName() );
        assertTrue( ((DummyLoadBalancingPlugin) plugin).wasInitialized );
    }

    @Test
    void shouldEnableShufflingOfDelegate() throws Throwable
    {
        // given
        Config config = Config.newBuilder()
                .set( CausalClusteringSettings.load_balancing_plugin, DUMMY_PLUGIN_NAME )
                .set( CausalClusteringSettings.load_balancing_shuffle, true ).build();

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderService.class ), NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof ServerShufflingProcessor );
        assertTrue( ((ServerShufflingProcessor) plugin).delegate() instanceof DummyLoadBalancingPlugin );
    }

    @Test
    void shouldFindServerPoliciesPlugin() throws Throwable
    {
        // given
        Config config = Config.newBuilder()
                .set( CausalClusteringSettings.load_balancing_plugin, ServerPoliciesPlugin.PLUGIN_NAME )
                .set( CausalClusteringSettings.load_balancing_shuffle, false ).build();

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderService.class ), NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof ServerPoliciesPlugin );
        assertEquals( ServerPoliciesPlugin.PLUGIN_NAME, ((ServerPoliciesPlugin) plugin).pluginName() );
    }

    @Test
    void serverPoliciesPluginShouldShuffleSelf() throws Throwable
    {
        // given
        Config config = Config.newBuilder()
                .set( CausalClusteringSettings.load_balancing_plugin, ServerPoliciesPlugin.PLUGIN_NAME )
                .set( CausalClusteringSettings.load_balancing_shuffle, true ).build();

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderService.class ), NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof ServerPoliciesPlugin );
        assertTrue( ((ServerPoliciesPlugin) plugin).isShufflingPlugin() );
    }

    @Test
    void shouldThrowOnInvalidPlugin()
    {
        assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, GraphDatabaseSettings.Mode.CORE )
                .set( CausalClusteringSettings.load_balancing_plugin, DOES_NOT_EXIST ).build() );
    }

    @Test
    void shouldNotAcceptInvalidSetting()
    {
        assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.strict_config_validation, true )
                .set( settingFor( DUMMY_PLUGIN_NAME, DummyLoadBalancingPlugin.DO_NOT_USE_THIS_CONFIG ), true )
                .set( CausalClusteringSettings.load_balancing_plugin, DUMMY_PLUGIN_NAME ).build() );
    }

    private static Setting<Boolean> settingFor( String pluginName, String settingName )
    {
        String name = String.format( "%s.%s.%s", "causal_clustering.load_balancing.config", pluginName, settingName );
        return newBuilder( name, BOOL, null ).build();
    }

    @ServiceProvider
    public static class DummyLoadBalancingPlugin implements LoadBalancingPlugin
    {
        static final String DO_NOT_USE_THIS_CONFIG = "do_not_use";
        boolean wasInitialized;

        public DummyLoadBalancingPlugin()
        {
        }

        @Override
        public void validate( Configuration configuration, Log log )
        {
            assertThrows( IllegalArgumentException.class, () -> configuration.get( settingFor( DUMMY_PLUGIN_NAME, DO_NOT_USE_THIS_CONFIG ) ) );
        }

        @Override
        public void init( TopologyService topologyService, LeaderService leaderService, LogProvider logProvider, Config config )
        {
            wasInitialized = true;
        }

        @Override
        public String pluginName()
        {
            return DUMMY_PLUGIN_NAME;
        }

        @Override
        public RoutingResult run( NamedDatabaseId namedDatabaseId, MapValue context )
        {
            return null;
        }
    }
}
