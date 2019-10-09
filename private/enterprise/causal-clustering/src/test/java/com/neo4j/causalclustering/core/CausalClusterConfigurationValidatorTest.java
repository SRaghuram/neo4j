/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.Mode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_type;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_label_selector;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_service_port_name;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_akka_external_config;
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
        Config config = Config.newBuilder()
                .set( EnterpriseEditionSettings.mode, Mode.SINGLE )
                .set( initial_discovery_members, Collections.emptyList() )
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
                .set( EnterpriseEditionSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
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
                .set( EnterpriseEditionSettings.mode, mode )
                .set( discovery_type, DiscoveryType.K8S )
                .set( kubernetes_label_selector, "waldo=fred" )
                .set( kubernetes_service_port_name, "default" )
                .set( BoltConnector.enabled, true )
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
                .set( EnterpriseEditionSettings.mode, mode )
                .set( initial_discovery_members, Collections.emptyList() )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
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
                .set( EnterpriseEditionSettings.mode, mode )
                .set( discovery_type, DiscoveryType.DNS )
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
                .set( EnterpriseEditionSettings.mode, mode )
                .set( discovery_type, DiscoveryType.LIST )
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
                .set( EnterpriseEditionSettings.mode, mode )
                .set( discovery_type, DiscoveryType.SRV )
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
                .set( EnterpriseEditionSettings.mode, mode )
                .set( discovery_type, DiscoveryType.K8S )
                .set( kubernetes_service_port_name, "default" )
                .set( BoltConnector.enabled, true )
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
                .set( EnterpriseEditionSettings.mode, mode )
                .set( discovery_type, DiscoveryType.K8S )
                .set( kubernetes_label_selector, "waldo=fred" )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class ).build();
    }

    @Test
    public void nonExistingAkkaExternalConfig()
    {
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage(
                "'causal_clustering.middleware.akka.external_config' must be a file or empty"
        );

        // when
        Config.newBuilder()
                .set( middleware_akka_external_config, Path.of( "this isnt a real file" ).toAbsolutePath() )
                .set( EnterpriseEditionSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();
    }

    @Test
    public void nonFileAkkaExternalConfig()
    {
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage(
                "'causal_clustering.middleware.akka.external_config' must be a file or empty"
        );

        // when
        Config.newBuilder()
                .set( middleware_akka_external_config, Path.of( "" ).toAbsolutePath() )
                .set( EnterpriseEditionSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();
    }

    @Test
    public void nonParseableAkkaExternalConfig() throws URISyntaxException
    {
        File conf = new File( getClass().getResource( "/akka.external.config/illegal.conf" ).toURI() );
        expected.expect( IllegalArgumentException.class );
        expected.expectMessage( String.format( "'%s' could not be parsed", conf ) );

        // when
        Config.newBuilder()
                .set( middleware_akka_external_config, conf.toPath() )
                .set( EnterpriseEditionSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();
    }

    @Test
    public void parseableAkkaExternalConfig() throws URISyntaxException
    {
        File conf = new File( getClass().getResource( "/akka.external.config/legal.conf" ).toURI() );

        // when
        Config.newBuilder()
                .set( middleware_akka_external_config, conf.toPath() )
                .set( EnterpriseEditionSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();

        // then no exception
    }
}
