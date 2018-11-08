/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Optional;

import org.neo4j.causalclustering.catchup.CatchupClientFactory;
import org.neo4j.causalclustering.catchup.MockCatchupClient;
import org.neo4j.causalclustering.catchup.MockCatchupClient.MockClientResponses;
import org.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV1;
import org.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV2;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV1;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV2;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.MockCatchupClient.responses;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.CATCHUP_1;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.CATCHUP_2;

@RunWith( value = Parameterized.class )
public class SnapshotDownloaderTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Protocol.ApplicationProtocol> data()
    {
        return Arrays.asList( CATCHUP_1, CATCHUP_2 );
    }

    @Parameterized.Parameter
    public Protocol.ApplicationProtocol protocol;

    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final AdvertisedSocketAddress remoteAddress = new AdvertisedSocketAddress( "localhost", 1234 );

    @Test
    public void shouldRequestSnapshot() throws Exception
    {
        // given
        CoreSnapshot expectedSnapshot = mock( CoreSnapshot.class );
        CatchupClientFactory catchupClientFactory = mockCatchupClient( responses().withCoreSnapshot( expectedSnapshot ) );

        // when
        SnapshotDownloader snapshotDownloader = new SnapshotDownloader( logProvider, catchupClientFactory );
        Optional<CoreSnapshot> downloadedSnapshot = snapshotDownloader.getCoreSnapshot( remoteAddress );

        // then
        assertTrue( downloadedSnapshot.isPresent() );
        assertEquals( expectedSnapshot, downloadedSnapshot.get() );
    }

    @Test
    public void shouldHandleFailure() throws Exception
    {
        // given
        CatchupClientFactory catchupClientFactory = mockCatchupClient( responses().withCoreSnapshot( () -> { throw new RuntimeException(); } ) );

        // when
        SnapshotDownloader downloader = new SnapshotDownloader( logProvider, catchupClientFactory );
        Optional<CoreSnapshot> downloadedSnapshot = downloader.getCoreSnapshot( remoteAddress );

        // then
        assertFalse( downloadedSnapshot.isPresent() );
    }

    private CatchupClientFactory mockCatchupClient( MockClientResponses clientResponses ) throws Exception
    {
        CatchupClientFactory catchupClientFactory = mock( CatchupClientFactory.class );

        CatchupClientV1 v1Client = new MockClientV1( clientResponses );
        CatchupClientV2 v2Client = new MockClientV2( clientResponses );

        VersionedCatchupClients catchupClient = new MockCatchupClient( protocol, v1Client, v2Client );
        when( catchupClientFactory.getClient( remoteAddress ) ).thenReturn( catchupClient );
        return catchupClientFactory;
    }
}
