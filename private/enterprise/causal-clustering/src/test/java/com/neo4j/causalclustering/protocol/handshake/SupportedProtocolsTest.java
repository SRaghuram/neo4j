/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.configuration.ApplicationProtocolVersion;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

class SupportedProtocolsTest
{
    private static final ApplicationProtocolVersion V1 = new ApplicationProtocolVersion( 1, 0 );
    private static final ApplicationProtocolVersion V2 = new ApplicationProtocolVersion( 2, 0 );
    private static final ApplicationProtocolVersion V3 = new ApplicationProtocolVersion( 3, 0 );
    private static final ApplicationProtocolVersion V4 = new ApplicationProtocolVersion( 4, 0 );
    private static final ApplicationProtocolVersion V5 = new ApplicationProtocolVersion( 5, 0 );
    private static final ApplicationProtocolVersion V6 = new ApplicationProtocolVersion( 6, 0 );

    @Test
    void shouldMutuallySupportIntersectionOfParameterVersionsSuperset()
    {
        // given
        var supportedProtocols = new ApplicationSupportedProtocols( RAFT, List.of( V1, V2 ) );

        // when
        var mutuallySupported = supportedProtocols.mutuallySupportedVersionsFor( Set.of( V1, V2, V3 ) );

        // then
        assertThat( mutuallySupported, containsInAnyOrder( V1, V2 ) );
    }

    @Test
    void shouldMutuallySupportIntersectionOfParameterVersionsSubset()
    {
        // given
        var supportedProtocols = new ApplicationSupportedProtocols( RAFT, List.of( V4, V5, V6 ) );

        // when
        var mutuallySupported = supportedProtocols.mutuallySupportedVersionsFor( Set.of( V4, V5 ) );

        // then
        assertThat( mutuallySupported, containsInAnyOrder( V4, V5 ) );
    }

    @Test
    void shouldMutuallySupportParameterIfEmptyVersions()
    {
        // given
        var supportedProtocols = new ApplicationSupportedProtocols( RAFT, emptyList() );

        // when
        var mutuallySupported = supportedProtocols.mutuallySupportedVersionsFor( Set.of( V5, V6 ) );

        // then
        assertThat( mutuallySupported, containsInAnyOrder( V5, V6 ) );
    }

    @Test
    void shouldMutuallySupportNothingIfParametersEmpty()
    {
        // given
        var supportedProtocols = new ApplicationSupportedProtocols( RAFT, List.of( V1, V2 ) );

        // when
        var mutuallySupported = supportedProtocols.mutuallySupportedVersionsFor( emptySet() );

        // then
        assertThat( mutuallySupported, empty() );
    }
}
