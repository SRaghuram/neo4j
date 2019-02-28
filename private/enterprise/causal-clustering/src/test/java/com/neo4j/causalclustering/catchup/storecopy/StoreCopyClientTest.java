/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.MockCatchupClient;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV1;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV2;
import com.neo4j.causalclustering.catchup.MockCatchupClient.MockClientV3;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV3;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import com.neo4j.causalclustering.helper.TimeoutStrategy;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.extension.SuppressOutputExtension;

import static com.neo4j.causalclustering.catchup.MockCatchupClient.responses;
import static com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV1;
import static com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV2;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.LAST_CHECKPOINTED_TX_UNAVAILABLE;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;
import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.CATCHUP_1;
import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.CATCHUP_2;
import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.CATCHUP_3;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( SuppressOutputExtension.class )
class StoreCopyClientTest
{
    private static final long LAST_CHECKPOINTED_TX = 11;

    public static Protocol.ApplicationProtocol[] protocols()
    {
        return new Protocol.ApplicationProtocol[]{CATCHUP_1, CATCHUP_2, CATCHUP_3};
    }

    private final CatchupClientFactory catchupClientFactory = mock( CatchupClientFactory.class );
    private final LogProvider logProvider = FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out );
    private final Monitors monitors = new Monitors();

    private StoreCopyClient subject;

    // params
    private final AdvertisedSocketAddress expectedAdvertisedAddress = new AdvertisedSocketAddress( "host", 1234 );
    private final CatchupAddressProvider catchupAddressProvider = CatchupAddressProvider.fromSingleAddress( expectedAdvertisedAddress );
    private final StoreId expectedStoreId = new StoreId( 1, 2, 3, 4 );
    private final StoreFileStreamProvider expectedStoreFileStream = mock( StoreFileStreamProvider.class );

    // helpers
    private File[] serverFiles = new File[]{new File( "fileA.txt" ), new File( "fileB.bmp" )};
    private File targetLocation = new File( "targetLocation" );
    private LongSet indexIds = LongSets.immutable.of( 13 );
    private ConstantTimeTimeoutStrategy backOffStrategy;
    private MockCatchupClient catchupClient;
    private final MockCatchupClient.MockClientResponses clientResponses = responses();
    private final CatchupClientV1 v1Client = spy( new MockClientV1( clientResponses ) );
    private final CatchupClientV2 v2Client = spy( new MockClientV2( clientResponses ) );
    private final CatchupClientV3 v3Client = spy( new MockClientV3( clientResponses ) );

    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    @ParameterizedTest( name = "{0}" )
    @MethodSource( "protocols" )
    @interface TestWithCatchupProtocols
    { }

    @BeforeEach
    void setup()
    {
        backOffStrategy = new ConstantTimeTimeoutStrategy( 1, TimeUnit.MILLISECONDS );
        subject = new StoreCopyClient( catchupClientFactory, DEFAULT_DATABASE_NAME, () -> monitors, logProvider, backOffStrategy );
    }

    private void mockClient( Protocol.ApplicationProtocol protocol ) throws Exception
    {
        catchupClient = new MockCatchupClient( protocol, v1Client, v2Client, v3Client );
        when( catchupClientFactory.getClient( any( AdvertisedSocketAddress.class ), any( Log.class ) ) ).thenReturn( catchupClient );
    }

    @TestWithCatchupProtocols
    void clientRequestsAllFilesListedInListingResponse( Protocol.ApplicationProtocol protocol ) throws Exception
    {
        // given
        mockClient( protocol );
        // setup fake catchup client responses. Lots of files, and any request for a store or index file will succeed
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, LAST_CHECKPOINTED_TX );
        StoreCopyFinishedResponse success = expectedStoreCopyFinishedResponse( SUCCESS, protocol );
        clientResponses
                .withPrepareStoreCopyResponse( prepareStoreCopyResponse )
                .withStoreFilesResponse( success )
                .withIndexFilesResponse( success );

        // when client requests catchup
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );

        // then there are as many requests to the server for individual requests
        List<String> filteredRequests = getRequestFileNames( protocol );
        List<String> expectedFiles = Stream.of( serverFiles ).map( File::getName ).collect( Collectors.toList() );
        assertThat( expectedFiles, containsInAnyOrder( filteredRequests.toArray() ) );
    }

    @TestWithCatchupProtocols
    void shouldRetrieveCorrectStoreIdForGivenDatabaseName( Protocol.ApplicationProtocol protocol ) throws Exception
    {
        // given
        mockClient( protocol );
        String altDbName = "alternative.db";
        StoreId defaultDbStoreId = new StoreId( 6, 3, 2, 6 );
        StoreId altDbStoreId = new StoreId( 4, 6,1,9 );
        Map<GetStoreIdRequest,StoreId> storeIdMap = new HashMap<>();
        storeIdMap.put( new GetStoreIdRequest( DEFAULT_DATABASE_NAME ), defaultDbStoreId );
        storeIdMap.put( new GetStoreIdRequest( altDbName ), altDbStoreId );
        clientResponses.withStoreId( storeIdMap::get );

        StoreCopyClient subjectA = subject;
        StoreCopyClient subjectB = new StoreCopyClient( catchupClientFactory, altDbName, () -> monitors, logProvider, backOffStrategy );

        // when client requests the remote store id for each database
        StoreId storeIdA = subjectA.fetchStoreId( expectedAdvertisedAddress );
        StoreId storeIdB = subjectB.fetchStoreId( expectedAdvertisedAddress );

        if ( catchupClient.protocol().equals( ApplicationProtocols.CATCHUP_2 ) || catchupClient.protocol().equals( CATCHUP_3 ) )
        {
            // then, if Catchup V2 is being used, store id matches
            assertEquals( storeIdA, defaultDbStoreId );
            assertEquals( storeIdB, altDbStoreId );
        }
        else
        {
            // else, if Catchup V1 is being used, both the storeId should always be that of the default database, neo4j
            assertEquals( storeIdA, defaultDbStoreId );
            assertEquals( storeIdB, defaultDbStoreId );
        }
    }

    @TestWithCatchupProtocols
    void shouldNotAwaitOnSuccess( Protocol.ApplicationProtocol protocol ) throws Exception
    {
        // given
        mockClient( protocol );
        TimeoutStrategy.Timeout mockedTimeout = mock( TimeoutStrategy.Timeout.class );
        TimeoutStrategy backoffStrategy = mock( TimeoutStrategy.class );
        when( backoffStrategy.newTimeout() ).thenReturn( mockedTimeout );

        subject = new StoreCopyClient( catchupClientFactory, DEFAULT_DATABASE_NAME, () -> monitors, logProvider, backoffStrategy );

        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, LAST_CHECKPOINTED_TX );
        StoreCopyFinishedResponse success = expectedStoreCopyFinishedResponse( SUCCESS, protocol );
        clientResponses
                .withPrepareStoreCopyResponse( prepareStoreCopyResponse )
                .withStoreFilesResponse( success )
                .withIndexFilesResponse( success );

        // when
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );

        // then
        verify( mockedTimeout, never() ).increment();
        verify( mockedTimeout, never() ).getMillis();
    }

    @TestWithCatchupProtocols
    void shouldFailIfTerminationConditionFails( Protocol.ApplicationProtocol protocol ) throws Exception
    {
        mockClient( protocol );
        // given a file will fail an expected number of times
        // and requesting the individual file will fail
        StoreCopyFinishedResponse failed = expectedStoreCopyFinishedResponse( E_TOO_FAR_BEHIND, protocol );
        // and the initial list+count store files request is successful
        PrepareStoreCopyResponse initialListingOfFilesResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, LAST_CHECKPOINTED_TX );

        clientResponses
                .withStoreFilesResponse( failed )
                .withPrepareStoreCopyResponse( initialListingOfFilesResponse );

        // when we perform catchup
        try
        {
            subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, () -> () ->
            {
                throw new StoreCopyFailedException( "This can't go on" );
            }, targetLocation );
            fail( "Expected exception: " + StoreCopyFailedException.class );
        }
        catch ( StoreCopyFailedException expectedException )
        {
            assertThat( expectedException.getMessage(), containsString( "This can't go on") );
            return;
        }

        fail( "Expected a StoreCopyFailedException" );
    }

    @TestWithCatchupProtocols
    void errorOnListingStore( Protocol.ApplicationProtocol protocol ) throws Exception
    {
        mockClient( protocol );
        // given store listing fails
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_LISTING_STORE );

        Queue<Supplier<PrepareStoreCopyResponse>> responses = new LinkedList<>();
        responses.add( () -> prepareStoreCopyResponse );
        responses.add( () ->
        {
            throw new RuntimeException( "Should not be accessible" );
        } );

        clientResponses.withPrepareStoreCopyResponse( ignored -> responses.poll().get() );

        // when
        StoreCopyFailedException exception = assertThrows( StoreCopyFailedException.class,
                () -> subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation ) );

        // then
        assertEquals( "Preparing store failed due to: E_LISTING_STORE", exception.getMessage() );
    }

    @TestWithCatchupProtocols
    void storeIdMismatchOnListing( Protocol.ApplicationProtocol protocol ) throws Exception
    {
        mockClient( protocol );
        // given store listing fails
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH );

        Queue<Supplier<PrepareStoreCopyResponse>> responses = new LinkedList<>();
        responses.add( () -> prepareStoreCopyResponse );
        responses.add( () ->
        {
            throw new RuntimeException( "Should not be accessible" );
        } );

        clientResponses.withPrepareStoreCopyResponse( ignored -> responses.poll().get() );

        // when
        StoreCopyFailedException exception = assertThrows( StoreCopyFailedException.class,
                () -> subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation ) );

        // then
        assertEquals( "Preparing store failed due to: E_STORE_ID_MISMATCH", exception.getMessage() );

    }

    @TestWithCatchupProtocols
    void storeFileEventsAreReported( Protocol.ApplicationProtocol protocol ) throws Exception
    {
        mockClient( protocol );
        // given
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, LAST_CHECKPOINTED_TX );
        StoreCopyFinishedResponse success = expectedStoreCopyFinishedResponse( SUCCESS, protocol );

        clientResponses
                .withPrepareStoreCopyResponse( prepareStoreCopyResponse )
                .withStoreFilesResponse( success )
                .withIndexFilesResponse( success );

        // and
        StoreCopyClientMonitor storeCopyClientMonitor = mock( StoreCopyClientMonitor.class );
        monitors.addMonitorListener( storeCopyClientMonitor );

        // when
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );

        // then
        verify( storeCopyClientMonitor ).startReceivingStoreFiles();
        for ( File storeFileRequested : serverFiles )
        {
            verify( storeCopyClientMonitor ).startReceivingStoreFile( Paths.get( targetLocation.toString(), storeFileRequested.toString() ).toString() );
            verify( storeCopyClientMonitor ).finishReceivingStoreFile( Paths.get( targetLocation.toString(), storeFileRequested.toString() ).toString() );
        }
        verify( storeCopyClientMonitor ).finishReceivingStoreFiles();
    }

    @TestWithCatchupProtocols
    void snapshotEventsAreReported( Protocol.ApplicationProtocol protocol ) throws Exception
    {
        mockClient( protocol );
        // given
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( serverFiles, indexIds, LAST_CHECKPOINTED_TX );
        StoreCopyFinishedResponse success = expectedStoreCopyFinishedResponse( SUCCESS, protocol );

        clientResponses
                .withPrepareStoreCopyResponse( prepareStoreCopyResponse )
                .withStoreFilesResponse( success )
                .withIndexFilesResponse( success );

        // and
        StoreCopyClientMonitor storeCopyClientMonitor = mock( StoreCopyClientMonitor.class );
        monitors.addMonitorListener( storeCopyClientMonitor );

        // when
        subject.copyStoreFiles( catchupAddressProvider, expectedStoreId, expectedStoreFileStream, continueIndefinitely(), targetLocation );

        // then
        verify( storeCopyClientMonitor ).startReceivingIndexSnapshots();
        LongIterator iterator = indexIds.longIterator();
        while ( iterator.hasNext() )
        {
            long indexSnapshotIdRequested = iterator.next();
            verify( storeCopyClientMonitor ).startReceivingIndexSnapshot( indexSnapshotIdRequested );
            verify( storeCopyClientMonitor ).finishReceivingIndexSnapshot( indexSnapshotIdRequested );
        }
        verify( storeCopyClientMonitor ).finishReceivingIndexSnapshots();
    }

    private List<String> getRequestFileNames( Protocol.ApplicationProtocol protocol )
    {

        ArgumentCaptor<File> fileArgumentCaptor = ArgumentCaptor.forClass( File.class );
        if ( protocol.equals( CATCHUP_1 ) )
        {
            verify( v1Client, atLeastOnce() ).getStoreFile( any( StoreId.class ), fileArgumentCaptor.capture(), anyLong() );
        }
        else if ( protocol.equals( CATCHUP_2 ) )
        {
            verify( v2Client, atLeastOnce() ).getStoreFile( any( StoreId.class ), fileArgumentCaptor.capture(), anyLong(), anyString() );
        }
        else
        {
            verify( v3Client, atLeastOnce() ).getStoreFile( any( StoreId.class ), fileArgumentCaptor.capture(), anyLong(), anyString() );
        }
        return fileArgumentCaptor.getAllValues().stream()
                .map( File::getName )
                .collect( Collectors.toList() );
    }

    private static StoreCopyFinishedResponse expectedStoreCopyFinishedResponse( StoreCopyFinishedResponse.Status status, Protocol.ApplicationProtocol protocol )
    {
        return new StoreCopyFinishedResponse( status, protocol == CATCHUP_3 ? LAST_CHECKPOINTED_TX : LAST_CHECKPOINTED_TX_UNAVAILABLE );
    }

    private Supplier<TerminationCondition> continueIndefinitely()
    {
        return () -> TerminationCondition.CONTINUE_INDEFINITELY;
    }
}

