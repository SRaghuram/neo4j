/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.neo4j.configuration.GraphDatabaseSettings.Mode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.discovery_type;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_label_selector;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_service_port_name;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_akka_external_config;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CausalClusterConfigurationValidatorTest
{
    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void validateOnlyIfModeIsCoreOrReplica( Mode mode )
    {
        // when
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, GraphDatabaseSettings.Mode.SINGLE )
                .set( initial_discovery_members, Collections.emptyList() )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();

        // then
        assertTrue( config.isExplicitlySet( initial_discovery_members ) );
        assertEquals( List.of(), config.get( initial_discovery_members ) );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void validateSuccessList( Mode mode )
    {
        // when
        Config config = Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();

        // then
        assertEquals( asList( new SocketAddress( "localhost", 99 ),
                new SocketAddress( "remotehost", 2 ) ),
                config.get( initial_discovery_members ) );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void validateSuccessKubernetes( Mode mode )
    {
        // when
        Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( discovery_type, DiscoveryType.K8S )
                .set( kubernetes_label_selector, "waldo=fred" )
                .set( kubernetes_service_port_name, "default" )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build();

        // then no exception
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void missingBoltConnector( Mode mode )
    {
        // when
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, Collections.emptyList() )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .addValidator( CausalClusterConfigurationValidator.class ).build() );

        assertThat( exception.getMessage() ).isEqualTo( "A Bolt connector must be configured to run a cluster" );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void missingInitialMembersDNS( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( discovery_type, DiscoveryType.DNS )
                .addValidator( CausalClusterConfigurationValidator.class ).build() );

        assertThat( exception.getMessage() )
                .isEqualTo( "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=DNS'" );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void missingInitialMembersLIST( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( discovery_type, DiscoveryType.LIST )
                .addValidator( CausalClusterConfigurationValidator.class ).build() );

        assertThat( exception.getMessage() ).isEqualTo(
                "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=LIST'" );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void missingInitialMembersSRV( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( discovery_type, DiscoveryType.SRV )
                .addValidator( CausalClusterConfigurationValidator.class ).build() );

        assertThat( exception.getMessage() )
                .isEqualTo( "Missing value for 'causal_clustering.initial_discovery_members', which is mandatory with 'causal_clustering.discovery_type=SRV'" );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void missingKubernetesLabelSelector( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( discovery_type, DiscoveryType.K8S )
                .set( kubernetes_service_port_name, "default" )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class ).build() );

        assertThat( exception.getMessage() )
                .isEqualTo( "Missing value for 'causal_clustering.kubernetes.label_selector', which is mandatory with 'causal_clustering.discovery_type=K8S'" );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void missingKubernetesPortName( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( GraphDatabaseSettings.mode, mode )
                .set( discovery_type, DiscoveryType.K8S )
                .set( kubernetes_label_selector, "waldo=fred" )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class ).build() );

        assertThat( exception.getMessage() ).isEqualTo(
                "Missing value for 'causal_clustering.kubernetes.service_port_name', which is mandatory with 'causal_clustering.discovery_type=K8S'" );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void nonExistingAkkaExternalConfig( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( middleware_akka_external_config, Path.of( "this isnt a real file" ).toAbsolutePath() )
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build() );

        assertThat( exception.getMessage() ).isEqualTo( "'causal_clustering.middleware.akka.external_config' must be a file or empty" );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void nonFileAkkaExternalConfig( Mode mode )
    {
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( middleware_akka_external_config, Path.of( "" ).toAbsolutePath() )
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build() );

        assertThat( exception.getMessage() ).isEqualTo( "'causal_clustering.middleware.akka.external_config' must be a file or empty" );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void nonParseableAkkaExternalConfig( Mode mode ) throws URISyntaxException
    {
        File conf = new File( getClass().getResource( "/akka.external.config/illegal.conf" ).toURI() );
        var exception = assertThrows( IllegalArgumentException.class, () -> Config.newBuilder()
                .set( middleware_akka_external_config, conf.toPath() )
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build() );

        assertThat( exception.getMessage() ).isEqualTo( String.format( "'%s' could not be parsed", conf ) );
    }

    @ParameterizedTest
    @EnumSource( value = Mode.class, mode = EnumSource.Mode.EXCLUDE, names = {"SINGLE"} )
    void parseableAkkaExternalConfig( Mode mode ) throws URISyntaxException
    {
        File conf = new File( getClass().getResource( "/akka.external.config/legal.conf" ).toURI() );

        // when
        assertDoesNotThrow( () -> Config.newBuilder()
                .set( middleware_akka_external_config, conf.toPath() )
                .set( GraphDatabaseSettings.mode, mode )
                .set( initial_discovery_members, List.of( new SocketAddress( "localhost", 99 ), new SocketAddress( "remotehost", 2 ) ) )
                .set( BoltConnector.enabled, true )
                .addValidator( CausalClusterConfigurationValidator.class )
                .build() );
    }
}
