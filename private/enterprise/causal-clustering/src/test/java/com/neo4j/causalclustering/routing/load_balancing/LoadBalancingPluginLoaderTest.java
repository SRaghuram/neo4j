/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.plugins.ServerShufflingProcessor;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerPoliciesPlugin;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class LoadBalancingPluginLoaderTest
{
    private static final String DUMMY_PLUGIN_NAME = "dummy";
    private static final String DOES_NOT_EXIST = "does_not_exist";

    @Test
    void shouldReturnSelectedPlugin() throws Throwable
    {
        // given
        Config config = Config.builder()
                .withSetting( CausalClusteringSettings.load_balancing_plugin, DUMMY_PLUGIN_NAME )
                .withSetting( CausalClusteringSettings.load_balancing_shuffle, "false" ).build();

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderService.class ),
                NullLogProvider.getInstance(),
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
        Config config = Config.builder()
                .withSetting( CausalClusteringSettings.load_balancing_plugin, DUMMY_PLUGIN_NAME )
                .withSetting( CausalClusteringSettings.load_balancing_shuffle, "true" ).build();

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderService.class ),
                NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof ServerShufflingProcessor );
        assertTrue( ((ServerShufflingProcessor) plugin).delegate() instanceof DummyLoadBalancingPlugin );
    }

    @Test
    void shouldFindServerPoliciesPlugin() throws Throwable
    {
        // given
        Config config = Config.builder()
                .withSetting( CausalClusteringSettings.load_balancing_plugin, ServerPoliciesPlugin.PLUGIN_NAME )
                .withSetting( CausalClusteringSettings.load_balancing_shuffle, "false" ).build();

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderService.class ),
                NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof ServerPoliciesPlugin );
        assertEquals( ServerPoliciesPlugin.PLUGIN_NAME, ((ServerPoliciesPlugin) plugin).pluginName() );
    }

    @Test
    void serverPoliciesPluginShouldShuffleSelf() throws Throwable
    {
        // given
        Config config = Config.builder()
                .withSetting( CausalClusteringSettings.load_balancing_plugin, ServerPoliciesPlugin.PLUGIN_NAME )
                .withSetting( CausalClusteringSettings.load_balancing_shuffle, "true" ).build();

        // when
        LoadBalancingProcessor plugin = LoadBalancingPluginLoader.load(
                mock( TopologyService.class ),
                mock( LeaderService.class ),
                NullLogProvider.getInstance(),
                config );

        // then
        assertTrue( plugin instanceof ServerPoliciesPlugin );
        assertTrue( ((ServerPoliciesPlugin) plugin).isShufflingPlugin() );
    }

    @Test
    void shouldThrowOnInvalidPlugin()
    {
        Config config = Config.defaults( CausalClusteringSettings.load_balancing_plugin, DOES_NOT_EXIST );

        assertThrows( InvalidSettingException.class, () -> LoadBalancingPluginLoader.validate( config, mock( Log.class ) ) );
    }

    @Test
    void shouldNotAcceptInvalidSetting()
    {
        Config config = Config.builder()
                .withSetting( settingFor( DUMMY_PLUGIN_NAME, DummyLoadBalancingPlugin.DO_NOT_USE_THIS_CONFIG ), "true")
                .withSetting( CausalClusteringSettings.load_balancing_plugin, DUMMY_PLUGIN_NAME ).build();

        assertThrows( InvalidSettingException.class, () -> LoadBalancingPluginLoader.validate( config, mock( Log.class ) ) );
    }

    private static String settingFor( String pluginName, String settingName )
    {
        return String.format( "%s.%s.%s", CausalClusteringSettings.load_balancing_config.name(), pluginName, settingName );
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
        public void validate( Config config, Log log ) throws InvalidSettingException
        {
            Optional<String> invalidSetting = config.getRaw( settingFor( DUMMY_PLUGIN_NAME, DO_NOT_USE_THIS_CONFIG ) );
            invalidSetting.ifPresent( s ->
            {
                throw new InvalidSettingException( "Do not use this setting" );
            } );
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
        public RoutingResult run( String databaseName, MapValue context )
        {
            return null;
        }
    }
}
