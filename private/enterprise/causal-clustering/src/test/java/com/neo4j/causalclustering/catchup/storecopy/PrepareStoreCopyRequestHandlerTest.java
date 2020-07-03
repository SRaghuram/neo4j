/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.collection.Dependencies;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseNameLogContext;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.storageengine.api.StoreId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

//TODO: Update with some test cases for issues related to databaseId
class PrepareStoreCopyRequestHandlerTest
{
    private static final StoreId STORE_ID_MATCHING = new StoreId( 1, 2, 3, 4, 5 );
    private static final StoreId STORE_ID_MISMATCHING = new StoreId( 6000, 7000, 8000, 9000, 10000 );
    private static final NamedDatabaseId NAMED_DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();
    private static final DatabaseId DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase().databaseId();

    private final ChannelHandlerContext channelHandlerContext = mock( ChannelHandlerContext.class );
    private final CheckPointer checkPointer = mock( CheckPointer.class );
    private final Database db = mock( Database.class );
    private final DatabaseAvailabilityGuard availabilityGuard = mock( DatabaseAvailabilityGuard.class );
    private final PrepareStoreCopyFiles prepareStoreCopyFiles = mock( PrepareStoreCopyFiles.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    private EmbeddedChannel embeddedChannel;
    private CatchupServerProtocol catchupServerProtocol;

    @BeforeEach
    void setup()
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( checkPointer );
        StoreCopyCheckPointMutex storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        when( db.getStoreCopyCheckPointMutex() ).thenReturn( storeCopyCheckPointMutex );
        when( db.getDependencyResolver() ).thenReturn( dependencies );
        when( availabilityGuard.isAvailable() ).thenReturn( true );
        when( db.getDatabaseAvailabilityGuard() ).thenReturn( availabilityGuard );
        when( db.getNamedDatabaseId() ).thenReturn( NAMED_DATABASE_ID );
        DatabaseLogService databaseLogService = new DatabaseLogService( new DatabaseNameLogContext( NAMED_DATABASE_ID ), new SimpleLogService( logProvider ) );
        when( db.getInternalLogProvider() ).thenReturn( databaseLogService.getInternalLogProvider() );
        embeddedChannel = new EmbeddedChannel( createHandler() );
    }

    @Test
    void shouldGiveErrorResponseIfStoreMismatch()
    {
        // given store id doesn't match

        // when PrepareStoreCopyRequest is written to channel
        embeddedChannel.writeInbound( new PrepareStoreCopyRequest( STORE_ID_MISMATCHING, DATABASE_ID  ) );

        // then there is a store id mismatch message
        assertEquals( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE, embeddedChannel.readOutbound() );
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH );
        assertEquals( response, embeddedChannel.readOutbound() );

        // and the expected message type is reset back to message type
        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    void shouldGetSuccessfulResponseFromPrepareStoreCopyRequest() throws Exception
    {
        // given storeId matches
        Path[] files = new Path[]{Path.of( "file" )};
        long lastCheckpoint = 1;

        configureProvidedStoreCopyFiles( new StoreResource[0], files, lastCheckpoint );

        // when store listing is requested
        embeddedChannel.writeInbound( channelHandlerContext, new PrepareStoreCopyRequest( STORE_ID_MATCHING, DATABASE_ID ) );

        // and the contents of the store listing response is sent
        assertEquals( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE, embeddedChannel.readOutbound() );
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.success( files, lastCheckpoint );
        assertEquals( response, embeddedChannel.readOutbound() );

        // and the protocol is reset to expect any message type after listing has been transmitted
        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    void shouldRetainLockWhileStreaming() throws Exception
    {
        // given
        ChannelPromise channelPromise = embeddedChannel.newPromise();
        ChannelHandlerContext channelHandlerContext = mock( ChannelHandlerContext.class );
        when( channelHandlerContext.writeAndFlush( any( PrepareStoreCopyResponse.class ) ) ).thenReturn( channelPromise );

        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        when( db.getStoreCopyCheckPointMutex() ).thenReturn( new StoreCopyCheckPointMutex( lock ) );
        PrepareStoreCopyRequestHandler subjectHandler = createHandler();

        // and
        Path[] files = new Path[]{Path.of( "file" )};
        long lastCheckpoint = 1;
        configureProvidedStoreCopyFiles( new StoreResource[0], files, lastCheckpoint );

        // when
        subjectHandler.channelRead0( channelHandlerContext, new PrepareStoreCopyRequest( STORE_ID_MATCHING, DATABASE_ID ) );

        // then
        assertEquals( 1, lock.getReadLockCount() );

        // when
        channelPromise.setSuccess();

        //then
        assertEquals( 0, lock.getReadLockCount() );
    }

    @Test
    void shouldFailToPrepareForStoreCopyWhenDatabaseIsNotAvailable()
    {
        // database is unavailable
        when( availabilityGuard.isAvailable() ).thenReturn( false );

        // prepare store copy request is sent
        embeddedChannel.writeInbound( new PrepareStoreCopyRequest( STORE_ID_MATCHING, DATABASE_ID ) );

        // error response should be returned
        assertEquals( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE, embeddedChannel.readOutbound() );
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_LISTING_STORE );
        assertEquals( response, embeddedChannel.readOutbound() );

        // warning should be logged
        assertThat( logProvider ).forClass( PrepareStoreCopyRequestHandler.class ).forLevel( WARN )
                .containsMessages( "Unable to prepare for store copy" );

        // and the protocol is reset to expect any message type after listing has been transmitted
        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    private PrepareStoreCopyRequestHandler createHandler()
    {
        catchupServerProtocol = new CatchupServerProtocol();
        catchupServerProtocol.expect( CatchupServerProtocol.State.PREPARE_STORE_COPY );
        when( db.getStoreId() ).thenReturn( STORE_ID_MATCHING );

        PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider = mock( PrepareStoreCopyFilesProvider.class );
        when( prepareStoreCopyFilesProvider.prepareStoreCopyFiles( any() ) ).thenReturn( prepareStoreCopyFiles );

        return new PrepareStoreCopyRequestHandler( catchupServerProtocol, db, prepareStoreCopyFilesProvider, 32768 );
    }

    private void configureProvidedStoreCopyFiles( StoreResource[] atomicFiles, Path[] files, long lastCommitedTx )
            throws IOException
    {
        when( prepareStoreCopyFiles.getAtomicFilesSnapshot() ).thenReturn( atomicFiles );
        when( prepareStoreCopyFiles.listReplayableFiles() ).thenReturn( files );
        when( checkPointer.lastCheckPointedTransactionId() ).thenReturn( lastCommitedTx );
    }
}
