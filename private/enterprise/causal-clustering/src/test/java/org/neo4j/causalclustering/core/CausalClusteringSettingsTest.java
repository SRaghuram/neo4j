/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.kernel.configuration.Settings;

import static org.junit.Assert.assertEquals;

public class CausalClusteringSettingsTest
{
    @Test
    public void shouldValidatePrefixBasedKeys()
    {
        // given
        BaseSetting<String> setting = Settings.prefixSetting( "foo", Settings.STRING, "" );

        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put( "foo.us_east_1c", "abcdef" );

        // when
        Map<String, String> validConfig = setting.validate( rawConfig, s ->
        {
        } );

        // then
        assertEquals( 1, validConfig.size() );
        assertEquals( rawConfig, validConfig );
    }

    @Test
    public void shouldValidateMultiplePrefixBasedKeys()
    {
        // given
        BaseSetting<String> setting = Settings.prefixSetting( "foo", Settings.STRING, "" );

        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put( "foo.us_east_1c", "abcdef" );
        rawConfig.put( "foo.us_east_1d", "ghijkl" );

        // when
        Map<String, String> validConfig = setting.validate( rawConfig, s ->
        {
        } );

        // then
        assertEquals( 2, validConfig.size() );
        assertEquals( rawConfig, validConfig );
    }

    @Test
    public void shouldValidateLoadBalancingServerPolicies()
    {
        // given
        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put( "causal_clustering.load_balancing.config.server_policies.us_east_1c", "all()" );

        // when
        Map<String, String> validConfig = CausalClusteringSettings.load_balancing_config.validate( rawConfig, s ->
        {
        } );

        // then
        assertEquals( 1, validConfig.size() );
        assertEquals( rawConfig, validConfig );
    }

    @Test
    public void shouldBeInvalidIfPrefixDoesNotMatch()
    {
        // given
        BaseSetting<String> setting = Settings.prefixSetting( "bar", Settings.STRING, "" );
        Map<String, String> rawConfig = new HashMap<>();
        rawConfig.put( "foo.us_east_1c", "abcdef" );

        // when
        Map<String, String> validConfig = setting.validate( rawConfig, s ->
        {
        } );

        // then
        assertEquals( 0, validConfig.size() );
    }
}
