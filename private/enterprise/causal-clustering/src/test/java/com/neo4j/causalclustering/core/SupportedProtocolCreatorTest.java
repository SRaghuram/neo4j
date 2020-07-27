/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import co.unruly.matchers.StreamMatchers;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory;
import com.neo4j.causalclustering.protocol.handshake.SupportedProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory;
import com.neo4j.configuration.ApplicationProtocolVersion;
import com.neo4j.configuration.CausalClusteringSettings;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocols.COMPRESSION_SNAPPY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SupportedProtocolCreatorTest
{
    private final NullLogProvider log = NullLogProvider.getInstance();

    @Test
    void shouldReturnRaftProtocol()
    {
        // given
        var config = Config.defaults();

        // when
        var supportedRaftProtocol = new SupportedProtocolCreator( config, log ).getSupportedRaftProtocolsFromConfiguration();

        // then
        assertThat( supportedRaftProtocol.identifier(), equalTo( ApplicationProtocolCategory.RAFT ) );
    }

    @Test
    void shouldReturnEmptyVersionSupportedRaftProtocolIfNoVersionsConfigured()
    {
        // given
        var config = Config.defaults();

        // when
        var supportedRaftProtocol = new SupportedProtocolCreator( config, log ).getSupportedRaftProtocolsFromConfiguration();

        // then
        assertThat( supportedRaftProtocol.versions(), empty() );
    }

    @Test
    void shouldFilterUnknownRaftImplementations()
    {
        // given
        var config = Config.defaults( CausalClusteringSettings.raft_implementations,
                List.of( appProtocolVer( 1, 0 ), appProtocolVer( 2, 0 ), appProtocolVer( 3, 0 ), appProtocolVer( 4, 0 ) ) );

        // when
        var supportedRaftProtocol = new SupportedProtocolCreator( config, log ).getSupportedRaftProtocolsFromConfiguration();

        // then
        assertThat( supportedRaftProtocol.versions(), contains( new ApplicationProtocolVersion( 2, 0 ), new ApplicationProtocolVersion( 3, 0 ) ) );
    }

    @Test
    void shouldReturnConfiguredRaftProtocolVersions()
    {
        // given
        var config = Config.defaults( CausalClusteringSettings.raft_implementations, List.of( appProtocolVer( 2, 0 ) ) );

        // when
        var supportedRaftProtocol = new SupportedProtocolCreator( config, log ).getSupportedRaftProtocolsFromConfiguration();

        // then
        assertThat( supportedRaftProtocol.versions(), contains( new ApplicationProtocolVersion( 2, 0 ) ) );
    }

    @Test
    void shouldThrowIfVersionsSpecifiedButAllUnknown()
    {
        // given
        var config = Config.defaults( CausalClusteringSettings.raft_implementations,  List.of( appProtocolVer(99999,99999 ) ) );
        var supportedProtocolCreator = new SupportedProtocolCreator( config, log );

        // when / then
        assertThrows( IllegalArgumentException.class, supportedProtocolCreator::getSupportedRaftProtocolsFromConfiguration );
    }

    @Test
    void shouldNotReturnModifiersIfNoVersionsSpecified()
    {
        // given
        var config = Config.defaults();

        // when
        var supportedModifierProtocols =
                new SupportedProtocolCreator( config, log ).createSupportedModifierProtocols();

        // then
        assertThat( supportedModifierProtocols, empty() );
    }

    @Test
    void shouldReturnACompressionModifierIfCompressionVersionsSpecified()
    {
        // given
        var config = Config.defaults( CausalClusteringSettings.compression_implementations, List.of( COMPRESSION_SNAPPY.implementation() ) );

        // when
        var supportedModifierProtocols =
                new SupportedProtocolCreator( config, log ).createSupportedModifierProtocols();

        // then
        var identifiers = supportedModifierProtocols.stream().map( SupportedProtocols::identifier );
        assertThat( identifiers, StreamMatchers.contains( ModifierProtocolCategory.COMPRESSION ) );
    }

    @Test
    void shouldReturnCompressionWithVersionsSpecified()
    {
        // given
        var config = Config.defaults( CausalClusteringSettings.compression_implementations, List.of( COMPRESSION_SNAPPY.implementation() ) );

        // when
        var supportedModifierProtocols =
                new SupportedProtocolCreator( config, log ).createSupportedModifierProtocols();

        // then
        var versions = supportedModifierProtocols.get( 0 ).versions();
        assertThat( versions, contains( COMPRESSION_SNAPPY.implementation() ) );
    }

    @Test
    void shouldReturnCompressionWithVersionsSpecifiedCaseInsensitive()
    {
        // given
        var config = Config.defaults( CausalClusteringSettings.compression_implementations, List.of( COMPRESSION_SNAPPY.implementation().toLowerCase() ) );

        // when
        var supportedModifierProtocols =
                new SupportedProtocolCreator( config, log ).createSupportedModifierProtocols();

        // then
        var versions = supportedModifierProtocols.get( 0 ).versions();
        assertThat( versions, contains( COMPRESSION_SNAPPY.implementation() ) );
    }

    private static ApplicationProtocolVersion appProtocolVer( int major, int minor )
    {
        return new ApplicationProtocolVersion( major, minor );
    }
}
