/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import co.unruly.matchers.OptionalMatchers;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import com.neo4j.configuration.ApplicationProtocolVersion;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Stream;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApplicationProtocolRepositoryTest
{
    private final ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository(
            TestApplicationProtocols.values(), new ApplicationSupportedProtocols( RAFT, TestApplicationProtocols.listVersionsOf( RAFT ) ) );

    @Test
    void shouldReturnEmptyIfUnknownVersion()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 42, 0 ) );

        // when
        var applicationProtocol = applicationProtocolRepository.select( RAFT.canonicalName(), versions );

        // then
        assertThat( applicationProtocol, OptionalMatchers.empty() );
    }

    @Test
    void shouldReturnEmptyIfUnknownName()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 1, 0 ) );

        // when
        var applicationProtocol = applicationProtocolRepository.select( "not a real protocol", versions );

        // then
        assertThat( applicationProtocol, OptionalMatchers.empty() );
    }

    @Test
    void shouldReturnEmptyIfNoVersions()
    {
        // given
        var versions = Set.<ApplicationProtocolVersion>of();

        // when
        var applicationProtocol = applicationProtocolRepository.select( RAFT.canonicalName(), versions );

        // then
        assertThat( applicationProtocol, OptionalMatchers.empty() );
    }

    @Test
    void shouldReturnProtocolIfKnownNameAndVersion()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 1, 0 ) );

        // when
        var applicationProtocol = applicationProtocolRepository.select( RAFT.canonicalName(), versions );

        // then
        assertThat( applicationProtocol, OptionalMatchers.contains( TestApplicationProtocols.RAFT_1 ) );
    }

    @Test
    void shouldReturnKnownProtocolVersionWhenFirstGivenVersionNotKnown()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 42, 0 ), new ApplicationProtocolVersion( 1, 0 ) );

        // when
        var applicationProtocol = applicationProtocolRepository.select( RAFT.canonicalName(), versions );

        // then
        assertThat( applicationProtocol, OptionalMatchers.contains( TestApplicationProtocols.RAFT_1 ) );
    }

    @Test
    void shouldReturnApplicationProtocolOfHighestVersionNumberRequestedAndSupported()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 389432, 0 ), new ApplicationProtocolVersion( 1, 0 ),
                new ApplicationProtocolVersion( 3, 0 ), new ApplicationProtocolVersion( 2, 0 ), new ApplicationProtocolVersion( 71234, 0 ) );

        // when
        var applicationProtocol = applicationProtocolRepository.select( RAFT.canonicalName(), versions );

        // then
        assertThat( applicationProtocol, OptionalMatchers.contains( TestApplicationProtocols.RAFT_3 ) );
    }

    @Test
    void shouldIncludeAllProtocolsInSelectionIfEmptyVersionsProvided()
    {
        // given
        var versions = Set.<ApplicationProtocolVersion>of();

        // when
        var protocolSelection = applicationProtocolRepository.getAll( RAFT, versions );

        // then
        var expectedRaftVersions = TestApplicationProtocols.allVersionsOf( RAFT );
        assertThat( protocolSelection.versions(), containsInAnyOrder( expectedRaftVersions ) );
    }

    @Test
    void shouldIncludeProtocolsInSelectionWithVersionsLimitedByThoseConfigured()
    {
        // given
        var versions = Set.of( new ApplicationProtocolVersion( 1, 0 ) );

        // when
        var protocolSelection = applicationProtocolRepository.getAll( RAFT, versions );

        // then
        assertEquals( versions, protocolSelection.versions() );
    }

    @Test
    void shouldIncludeProtocolsInSelectionWithVersionsLimitedByThoseExisting()
    {
        // given
        var expectedRaftVersions = TestApplicationProtocols.allVersionsOf( RAFT );
        var configuredRaftVersions = Stream.concat( Stream.of( expectedRaftVersions ),
                Stream.of( new ApplicationProtocolVersion( Integer.MAX_VALUE, 0 ) ) ).collect( toList() );

        // when
        var protocolSelection = applicationProtocolRepository.getAll( RAFT, configuredRaftVersions );

        // then
        assertThat( protocolSelection.versions(), containsInAnyOrder( expectedRaftVersions ) );
    }

    @Test
    void shouldThrowIfNoIntersectionBetweenExistingAndConfiguredVersions()
    {
        // given
        var configuredRaftVersions = Set.of( new ApplicationProtocolVersion( Integer.MAX_VALUE, 0 ) );

        // when / then
        assertThrows( IllegalArgumentException.class, () -> applicationProtocolRepository.getAll( RAFT, configuredRaftVersions ) );
    }

    @Test
    void shouldNotInstantiateIfDuplicateProtocolsSupplied()
    {
        // given
        var protocol = new ApplicationProtocol()
        {
            @Override
            public String category()
            {
                return "foo";
            }

            @Override
            public ApplicationProtocolVersion implementation()
            {
                return new ApplicationProtocolVersion( 1, 0 );
            }
        };
        var protocols = new ApplicationProtocol[]{protocol, protocol};

        // when / then
        assertThrows( IllegalArgumentException.class, () ->
                new ApplicationProtocolRepository( protocols, new ApplicationSupportedProtocols( RAFT, TestApplicationProtocols.listVersionsOf( RAFT ) ) ) );
    }
}
