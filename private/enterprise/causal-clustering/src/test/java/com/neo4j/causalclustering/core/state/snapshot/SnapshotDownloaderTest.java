/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.snapshot;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.MockCatchupClient;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientResponses;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV3;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV3;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Optional;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.catchup.MockCatchupClient.responses;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.CATCHUP;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( value = Parameterized.class )
public class SnapshotDownloaderTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<ApplicationProtocol> data()
    {
        return ApplicationProtocols.withCategory( CATCHUP );
    }

    @Parameterized.Parameter
    public ApplicationProtocol protocol;

    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final SocketAddress remoteAddress = new SocketAddress( "localhost", 1234 );
    private final DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @Test
    public void shouldRequestSnapshot() throws Exception
    {
        // given
        CoreSnapshot expectedSnapshot = mock( CoreSnapshot.class );
        CatchupClientFactory catchupClientFactory = mockCatchupClient( responses().withCoreSnapshot( expectedSnapshot ) );

        // when
        SnapshotDownloader snapshotDownloader = new SnapshotDownloader( logProvider, catchupClientFactory );
        Optional<CoreSnapshot> downloadedSnapshot = snapshotDownloader.getCoreSnapshot( databaseIdRepository.get( "database_name" ), remoteAddress );

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
        Optional<CoreSnapshot> downloadedSnapshot = downloader.getCoreSnapshot( databaseIdRepository.get( "database_name" ), remoteAddress );

        // then
        assertFalse( downloadedSnapshot.isPresent() );
    }

    private CatchupClientFactory mockCatchupClient( MockClientResponses clientResponses ) throws Exception
    {
        CatchupClientFactory catchupClientFactory = mock( CatchupClientFactory.class );

        CatchupClientV3 v3Client = new MockClientV3( clientResponses, databaseIdRepository );

        VersionedCatchupClients catchupClient = new MockCatchupClient( protocol, v3Client );
        when( catchupClientFactory.getClient( eq( remoteAddress ), any( Log.class ) ) ).thenReturn( catchupClient );
        return catchupClientFactory;
    }
}
