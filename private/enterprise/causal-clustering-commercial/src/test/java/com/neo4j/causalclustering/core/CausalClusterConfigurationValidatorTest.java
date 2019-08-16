/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.neo4j.causalclustering.core.DiscoveryType;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.Mode;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.discovery_type;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_label_selector;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_service_port_name;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.middleware_akka_external_config;

@RunWith( Parameterized.class )
public class CausalClusterConfigurationValidatorTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Parameterized.Parameter
    public Mode mode;

    @Parameterized.Parameters( name = "{0}" )
    public static List<Mode> recordFormats()
    {
        return Arrays.asList( Mode.CORE, Mode.READ_REPLICA );
    }

    @Test
    public void validateOnlyIfModeIsCoreOrReplica()
    {
        // when
        Config config = Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, Mode.SINGLE.name() )
                .withSetting( initial_discovery_members, "" )
                .withValidator( new CausalClusterConfigurationValidator() )
                .build();

        // then
        Optional<String> value = config.getRaw( initial_discovery_members.name() );
        assertTrue( value.isPresent() );
        assertEquals( "", value.get() );
    }

    @Test
    public void validateSuccessList()
    {
        // when
        Config config = Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( initial_discovery_members, "localhost:99,remotehost:2" )
                .withSetting( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withValidator( new CausalClusterConfigurationValidator() )
                .build();

        // then
        assertEquals( asList( new AdvertisedSocketAddress( "localhost", 99 ),
                new AdvertisedSocketAddress( "remotehost", 2 ) ),
                config.get( initial_discovery_members ) );
    }

    @Test
    public void validateSuccessKubernetes()
    {
        // when
        Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( discovery_type, DiscoveryType.K8S.name() )
                .withSetting( kubernetes_label_selector, "waldo=fred" )
                .withSetting( kubernetes_service_port_name, "default" )
                .withSetting( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withValidator( new CausalClusterConfigurationValidator() )
                .build();

        // then no exception
    }

    @Test
    public void missingBoltConnector()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "A Bolt connector must be configured to run a cluster" );

        // when
        Config.builder()
                .withSetting( EnterpriseEditionSettings.mode.name(), mode.name() )
                .withSetting( initial_discovery_members, "" )
                .withSetting( initial_discovery_members.name(), "localhost:99,remotehost:2" )
                .withValidator( new CausalClusterConfigurationValidator() ).build();
    }

    @Test
    public void missingInitialMembersDNS()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=DNS'"
        );

        // when
        Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( discovery_type, DiscoveryType.DNS.name() )
                .withValidator( new CausalClusterConfigurationValidator() ).build();
    }

    @Test
    public void missingInitialMembersLIST()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=LIST'" );

        // when
        Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( discovery_type, DiscoveryType.LIST.name() )
                .withValidator( new CausalClusterConfigurationValidator() ).build();
    }

    @Test
    public void missingInitialMembersSRV()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=SRV'" );

        // when
        Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( discovery_type, DiscoveryType.SRV.name() )
                .withValidator( new CausalClusterConfigurationValidator() ).build();
    }

    @Test
    public void missingKubernetesLabelSelector()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.kubernetes.label_selector', which is mandatory with 'causal_clustering.discovery_type=K8S'"
        );

        // when
        Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( discovery_type, DiscoveryType.K8S.name() )
                .withSetting( kubernetes_service_port_name, "default" )
                .withSetting( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withValidator( new CausalClusterConfigurationValidator() ).build();
    }

    @Test
    public void missingKubernetesPortName()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.kubernetes.service_port_name', which is mandatory with 'causal_clustering.discovery_type=K8S'"
        );

        // when
        Config.builder()
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( discovery_type, DiscoveryType.K8S.name() )
                .withSetting( kubernetes_label_selector, "waldo=fred" )
                .withSetting( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withValidator( new CausalClusterConfigurationValidator() ).build();
    }

    @Test
    public void nonExistingAkkaExternalConfig()
    {
        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "'causal_clustering.middleware.akka.external_config' must be a file or empty"
        );

        // when
        Config.builder()
                .withSetting( middleware_akka_external_config, "this isnt a real file" )
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( initial_discovery_members, "localhost:99,remotehost:2" )
                .withSetting( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withValidator( new CausalClusterConfigurationValidator() )
                .build();
    }

    @Test
    public void nonFileAkkaExternalConfig()
    {
        expected.expect( InvalidSettingException.class );
        expected.expectMessage(
                "'causal_clustering.middleware.akka.external_config' must be a file or empty"
        );

        // when
        Config.builder()
                .withSetting( middleware_akka_external_config, "/" )
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( initial_discovery_members, "localhost:99,remotehost:2" )
                .withSetting( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withValidator( new CausalClusterConfigurationValidator() )
                .build();
    }

    @Test
    public void nonParseableAkkaExternalConfig() throws URISyntaxException
    {
        File conf = new File( getClass().getResource( "/akka.external.config/illegal.conf" ).toURI() );
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( String.format( "'%s' could not be parsed", conf ) );

        // when
        Config.builder()
                .withSetting( middleware_akka_external_config, conf.toString() )
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( initial_discovery_members, "localhost:99,remotehost:2" )
                .withSetting( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withValidator( new CausalClusterConfigurationValidator() )
                .build();
    }

    @Test
    public void parseableAkkaExternalConfig() throws URISyntaxException
    {
        File conf = new File( getClass().getResource( "/akka.external.config/legal.conf" ).toURI() );

        // when
        Config.builder()
                .withSetting( middleware_akka_external_config, conf.toString() )
                .withSetting( EnterpriseEditionSettings.mode, mode.name() )
                .withSetting( initial_discovery_members, "localhost:99,remotehost:2" )
                .withSetting( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withValidator( new CausalClusterConfigurationValidator() )
                .build();

        // then no exception
    }
}
