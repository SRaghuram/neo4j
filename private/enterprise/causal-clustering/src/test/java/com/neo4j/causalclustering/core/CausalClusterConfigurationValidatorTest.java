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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_type;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_label_selector;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_service_port_name;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

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
        Config config = Config.newBuilder()
                .set( CommercialEditionSettings.mode, Mode.SINGLE.name() )
                .set( initial_discovery_members, "" )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();

        // then
        assertTrue( config.isExplicitlySet( initial_discovery_members ) );
        assertEquals( List.of() , config.get( initial_discovery_members ) );
    }

    @Test
    public void validateSuccessList()
    {
        // when
        Config config = Config.newBuilder()
                .set( CommercialEditionSettings.mode, Mode.SINGLE.name() )
                .set( initial_discovery_members, "localhost:99,remotehost:2" )
                .set( BoltConnector.group( "bolt" ).enabled, TRUE )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();

        // then
        assertEquals( asList( new SocketAddress( "localhost", 99 ),
                new SocketAddress( "remotehost", 2 ) ),
                config.get( initial_discovery_members ) );
    }

    @Test
    public void validateSuccessKubernetes()
    {
        // when
        Config.newBuilder()
                .set( CommercialEditionSettings.mode, Mode.SINGLE.name() )
                .set( discovery_type, DiscoveryType.K8S.name() )
                .set( kubernetes_label_selector, "waldo=fred" )
                .set( kubernetes_service_port_name, "default" )
                .set( BoltConnector.group( "bolt" ).enabled, TRUE )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();

        // then no exception
    }

    @Test
    public void missingBoltConnector()
    {
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage( "A Bolt connector must be configured to run a cluster" );

        // when
        Config.newBuilder()
                .set( CommercialEditionSettings.mode, mode.name() )
                .set( initial_discovery_members, "" )
                .set( initial_discovery_members, "localhost:99,remotehost:2" )
                .addValidator( CausalClusterConfigurationValidator.class ).build();
    }

    @Test
    public void missingInitialMembersDNS()
    {
        // then
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=DNS'"
        );

        // when
        Config.newBuilder()
                .set( CommercialEditionSettings.mode, mode.name() )
                .set( discovery_type, DiscoveryType.DNS.name() )
                .addValidator( CausalClusterConfigurationValidator.class ).build();
    }

    @Test
    public void missingInitialMembersLIST()
    {
        // then
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=LIST'" );

        // when
        Config.newBuilder()
                .set( CommercialEditionSettings.mode, mode.name() )
                .set( discovery_type, DiscoveryType.LIST.name() )
                .addValidator( CausalClusterConfigurationValidator.class ).build();
    }

    @Test
    public void missingInitialMembersSRV()
    {
        // then
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=SRV'" );

        // when
        Config.newBuilder()
                .set( CommercialEditionSettings.mode, mode.name() )
                .set( discovery_type, DiscoveryType.SRV.name() )
                .addValidator( CausalClusterConfigurationValidator.class ).build();
    }

    @Test
    public void missingKubernetesLabelSelector()
    {
        // then
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.kubernetes.label_selector', which is mandatory with 'causal_clustering.discovery_type=K8S'"
        );

        // when
        Config.newBuilder()
                .set( CommercialEditionSettings.mode, mode.name() )
                .set( discovery_type, DiscoveryType.K8S.name() )
                .set( kubernetes_service_port_name, "default" )
                .set( BoltConnector.group( "bolt" ).enabled, TRUE )
                .addValidator( CausalClusterConfigurationValidator.class ).build();
    }

    @Test
    public void missingKubernetesPortName()
    {
        // then
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage(
                "Missing value for 'causal_clustering.kubernetes.service_port_name', which is mandatory with 'causal_clustering.discovery_type=K8S'"
        );

        // when
        Config.newBuilder()
                .set( CommercialEditionSettings.mode, mode.name() )
                .set( discovery_type, DiscoveryType.K8S.name() )
                .set( kubernetes_label_selector, "waldo=fred" )
                .set( BoltConnector.group( "bolt" ).enabled, TRUE )
                .addValidator( CausalClusterConfigurationValidator.class ).build();
    }
}
