/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;
import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.Mode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_type;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_label_selector;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_service_port_name;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
                .withSetting( CommercialEditionSettings.mode, Mode.SINGLE.name() )
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
                .withSetting( CommercialEditionSettings.mode, Mode.SINGLE.name() )
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
                .withSetting( CommercialEditionSettings.mode, Mode.SINGLE.name() )
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
                .withSetting( CommercialEditionSettings.mode.name(), mode.name() )
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
                .withSetting( CommercialEditionSettings.mode, mode.name() )
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
                .withSetting( CommercialEditionSettings.mode, mode.name() )
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
                .withSetting( CommercialEditionSettings.mode, mode.name() )
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
                .withSetting( CommercialEditionSettings.mode, mode.name() )
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
                .withSetting( CommercialEditionSettings.mode, mode.name() )
                .withSetting( discovery_type, DiscoveryType.K8S.name() )
                .withSetting( kubernetes_label_selector, "waldo=fred" )
                .withSetting( new BoltConnector( "bolt" ).enabled.name(), "true" )
                .withValidator( new CausalClusterConfigurationValidator() ).build();
    }
}
