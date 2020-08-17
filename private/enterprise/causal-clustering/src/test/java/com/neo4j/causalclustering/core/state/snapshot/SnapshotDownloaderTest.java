/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.MockCatchupClient;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientResponses;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV3;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV4;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.catchup.MockCatchupClient.responses;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.CATCHUP;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.CATCHUP_3_0;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.CATCHUP_4_0;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class SnapshotDownloaderTest
{
    static Iterable<ApplicationProtocol> data()
    {
        return ApplicationProtocols.withCategory( CATCHUP );
    }

    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final SocketAddress remoteAddress = new SocketAddress( "localhost", 1234 );
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldRequestSnapshot( ApplicationProtocol protocol ) throws Exception
    {
        // given
        CoreSnapshot expectedSnapshot = mock( CoreSnapshot.class );
        CatchupClientFactory catchupClientFactory = mockCatchupClient( responses().withCoreSnapshot( expectedSnapshot ), protocol );

        // when
        SnapshotDownloader snapshotDownloader = new SnapshotDownloader( logProvider, catchupClientFactory );
        Optional<CoreSnapshot> downloadedSnapshot = snapshotDownloader.getCoreSnapshot( databaseIdRepository.getRaw( "database_name" ), remoteAddress );

        // then
        assertTrue( downloadedSnapshot.isPresent() );
        assertEquals( expectedSnapshot, downloadedSnapshot.get() );
    }

    @ParameterizedTest
    @MethodSource( "data" )
    void shouldHandleFailure( ApplicationProtocol protocol ) throws Exception
    {
        // given
        CatchupClientFactory catchupClientFactory = mockCatchupClient( responses().withCoreSnapshot( () ->
        {
            throw new RuntimeException();
        } ), protocol );

        // when
        SnapshotDownloader downloader = new SnapshotDownloader( logProvider, catchupClientFactory );
        Optional<CoreSnapshot> downloadedSnapshot = downloader.getCoreSnapshot( databaseIdRepository.getRaw( "database_name" ), remoteAddress );

        // then
        assertFalse( downloadedSnapshot.isPresent() );
    }

    private CatchupClientFactory mockCatchupClient( MockClientResponses clientResponses,
                                                    ApplicationProtocol protocol ) throws Exception
    {
        CatchupClientFactory catchupClientFactory = mock( CatchupClientFactory.class );
        MockCatchupClient catchupClient;
        if ( protocol.equals( CATCHUP_3_0 ) )
        {
            MockClientV3 v3 = spy( new MockClientV3( clientResponses, databaseIdRepository ) );
            catchupClient = new MockCatchupClient( protocol, v3 );
        }
        else if ( protocol.equals( CATCHUP_4_0 ) )
        {
            MockClientV4 v4 = spy( new MockClientV4( clientResponses, databaseIdRepository ) );
            catchupClient = new MockCatchupClient( protocol, v4 );
        }
        else
        {
            throw new IllegalStateException( protocol.implementation() + " is not initialized" );
        }
        when( catchupClientFactory.getClient( any( SocketAddress.class ), any( Log.class ) ) ).thenReturn( catchupClient );
        return catchupClientFactory;
    }
}
