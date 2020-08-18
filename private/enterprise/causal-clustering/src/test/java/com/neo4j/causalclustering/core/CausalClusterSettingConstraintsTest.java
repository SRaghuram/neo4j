/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.configuration.DiscoveryType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.Mode;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.configuration.CausalClusteringInternalSettings.middleware_akka_external_config;
import static com.neo4j.configuration.CausalClusteringSettings.discovery_type;
import static com.neo4j.configuration.CausalClusteringSettings.initial_discovery_members;
import static com.neo4j.configuration.CausalClusteringSettings.kubernetes_label_selector;
import static com.neo4j.configuration.CausalClusteringSettings.kubernetes_service_port_name;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalClusterSettingConstraintsTest
{
    @TestClusterMode
    void validateOnlyIfModeIsCoreOrReplica( Mode mode )
    {
        // when
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, GraphDatabaseSettings.Mode.SINGLE )
                .set( initial_discovery_members, Collections.emptyList() )
                .build();

        // then
        assertTrue( config.isExplicitlySet( initial_discovery_members ) );
        assertEquals( List.of(), config.get( initial_discovery_members ) );
    }

    @TestClusterMode
    void validateSuccessList( Mode mode )
    {
        // when
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .build();

        // then
        assertEquals( asList( new SocketAddress( "localhost", 99 ),
                new SocketAddress( "remotehost", 2 ) ),
                config.get( initial_discovery_members ) );
    }

    @TestClusterMode
    void validateSuccessKubernetes( Mode mode )
    {
        // when
        Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( discovery_type, DiscoveryType.K8S )
                .set( kubernetes_label_selector, "waldo=fred" )
                .set( kubernetes_service_port_name, "default" )
                .set( BoltConnector.enabled, true )
                .build();

        // then no exception
    }

    @TestClusterMode
    void missingInitialMembersDNS( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( BoltConnector.enabled, true )
                .set( discovery_type, DiscoveryType.DNS ).build() );

        assertThat( exception.getMessage() )
                .endsWith( "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=DNS'" );
    }

    @TestClusterMode
    void missingInitialMembersLIST( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( BoltConnector.enabled, true )
                .set( discovery_type, DiscoveryType.LIST ).build() );

        assertThat( exception.getMessage() ).endsWith(
                "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=LIST'" );
    }

    @TestClusterMode
    void missingInitialMembersSRV( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( BoltConnector.enabled, true )
                .set( discovery_type, DiscoveryType.SRV ).build() );

        assertThat( exception.getMessage() )
                .endsWith( "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=SRV'" );
    }

    @TestClusterMode
    void missingKubernetesLabelSelector( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( discovery_type, DiscoveryType.K8S )
                .set( kubernetes_service_port_name, "default" )
                .set( BoltConnector.enabled, true ).build() );

        assertThat( exception.getMessage() )
                .endsWith( "Missing value for 'causal_clustering.kubernetes.label_selector', which is mandatory with 'causal_clustering.discovery_type=K8S'" );
    }

    @TestClusterMode
    void missingKubernetesPortName( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( discovery_type, DiscoveryType.K8S )
                .set( kubernetes_label_selector, "waldo=fred" )
                .set( BoltConnector.enabled, true ).build() );

        assertThat( exception.getMessage() ).endsWith(
                "Missing value for 'causal_clustering.kubernetes.service_port_name', which is mandatory with 'causal_clustering.discovery_type=K8S'" );
    }

    @TestClusterMode
    void nonExistingAkkaExternalConfig( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( middleware_akka_external_config, Path.of( "this isnt a real file" ).toAbsolutePath() )
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .build() );

        assertThat( exception.getMessage() ).endsWith( "'causal_clustering.middleware.akka.external_config' must be a file or empty" );
    }

    @TestClusterMode
    void nonFileAkkaExternalConfig( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( middleware_akka_external_config, Path.of( "" ).toAbsolutePath() )
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .build() );

        assertThat( exception.getMessage() ).endsWith( "'causal_clustering.middleware.akka.external_config' must be a file or empty" );
    }

    @TestClusterMode
    void nonParseableAkkaExternalConfig( Mode mode ) throws URISyntaxException
    {
        Path conf = Path.of( getClass().getResource( "/akka.external.config/illegal.conf" ).toURI() );
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( middleware_akka_external_config, conf )
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .build() );

        assertThat( exception.getMessage() ).endsWith( String.format( "'%s' could not be parsed", conf ) );
    }

    @TestClusterMode
    void parseableAkkaExternalConfig( Mode mode ) throws URISyntaxException
    {
        Path conf = Path.of( getClass().getResource( "/akka.external.config/legal.conf" ).toURI() );

        // when
        assertDoesNotThrow( () -> Config.newBuilder()
                .set( middleware_akka_external_config, conf )
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .build() );
    }

    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    @interface TestClusterMode
    {
    }
}
