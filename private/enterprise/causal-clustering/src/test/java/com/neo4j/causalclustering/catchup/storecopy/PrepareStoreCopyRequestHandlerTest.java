/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.identity.StoreId;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.inLog;

//TODO: Update with some test cases for issues related to databaseName
public class PrepareStoreCopyRequestHandlerTest
{
    private static final StoreId STORE_ID_MATCHING = new StoreId( 1, 2, 3, 4 );
    private static final StoreId STORE_ID_MISMATCHING = new StoreId( 5000, 6000, 7000, 8000 );

    private final ChannelHandlerContext channelHandlerContext = mock( ChannelHandlerContext.class );
    private final CheckPointer checkPointer = mock( CheckPointer.class );
    private final Database db = mock( Database.class );
    private final DatabaseAvailabilityGuard availabilityGuard = mock( DatabaseAvailabilityGuard.class );
    private final PrepareStoreCopyFiles prepareStoreCopyFiles = mock( PrepareStoreCopyFiles.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    private EmbeddedChannel embeddedChannel;
    private CatchupServerProtocol catchupServerProtocol;

    @Before
    public void setup()
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( checkPointer );
        StoreCopyCheckPointMutex storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        when( db.getStoreCopyCheckPointMutex() ).thenReturn( storeCopyCheckPointMutex );
        when( db.getDependencyResolver() ).thenReturn( dependencies );
        when( availabilityGuard.isAvailable() ).thenReturn( true );
        when( db.getDatabaseAvailabilityGuard() ).thenReturn( availabilityGuard );
        embeddedChannel = new EmbeddedChannel( createHandler() );
    }

    @Test
    public void shouldGiveErrorResponseIfStoreMismatch()
    {
        // given store id doesn't match

        // when PrepareStoreCopyRequest is written to channel
        embeddedChannel.writeInbound( new PrepareStoreCopyRequest( STORE_ID_MISMATCHING, DEFAULT_DATABASE_NAME  ) );

        // then there is a store id mismatch message
        assertEquals( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE, embeddedChannel.readOutbound() );
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH );
        assertEquals( response, embeddedChannel.readOutbound() );

        // and the expected message type is reset back to message type
        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    public void shouldGetSuccessfulResponseFromPrepareStoreCopyRequest() throws Exception
    {
        // given storeId matches
        LongSet indexIds = LongSets.immutable.of( 1 );
        File[] files = new File[]{new File( "file" )};
        long lastCheckpoint = 1;

        configureProvidedStoreCopyFiles( new StoreResource[0], files, indexIds, lastCheckpoint );

        // when store listing is requested
        embeddedChannel.writeInbound( channelHandlerContext, new PrepareStoreCopyRequest( STORE_ID_MATCHING, DEFAULT_DATABASE_NAME ) );

        // and the contents of the store listing response is sent
        assertEquals( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE, embeddedChannel.readOutbound() );
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.success( files, indexIds, lastCheckpoint );
        assertEquals( response, embeddedChannel.readOutbound() );

        // and the protocol is reset to expect any message type after listing has been transmitted
        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    public void shouldRetainLockWhileStreaming() throws Exception
    {
        // given
        ChannelPromise channelPromise = embeddedChannel.newPromise();
        ChannelHandlerContext channelHandlerContext = mock( ChannelHandlerContext.class );
        when( channelHandlerContext.writeAndFlush( any( PrepareStoreCopyResponse.class ) ) ).thenReturn( channelPromise );

        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        when( db.getStoreCopyCheckPointMutex() ).thenReturn( new StoreCopyCheckPointMutex( lock ) );
        PrepareStoreCopyRequestHandler subjectHandler = createHandler();

        // and
        LongSet indexIds = LongSets.immutable.of( 42 );
        File[] files = new File[]{new File( "file" )};
        long lastCheckpoint = 1;
        configureProvidedStoreCopyFiles( new StoreResource[0], files, indexIds, lastCheckpoint );

        // when
        subjectHandler.channelRead0( channelHandlerContext, new PrepareStoreCopyRequest( STORE_ID_MATCHING, DEFAULT_DATABASE_NAME ) );

        // then
        assertEquals( 1, lock.getReadLockCount() );

        // when
        channelPromise.setSuccess();

        //then
        assertEquals( 0, lock.getReadLockCount() );
    }

    @Test
    public void shouldFailToPrepareForStoreCopyWhenDatabaseIsNotAvailable()
    {
        // database is unavailable
        when( availabilityGuard.isAvailable() ).thenReturn( false );

        // prepare store copy request is sent
        embeddedChannel.writeInbound( new PrepareStoreCopyRequest( STORE_ID_MATCHING, DEFAULT_DATABASE_NAME ) );

        // error response should be returned
        assertEquals( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE, embeddedChannel.readOutbound() );
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_LISTING_STORE );
        assertEquals( response, embeddedChannel.readOutbound() );

        // warning should be logged
        logProvider.containsMatchingLogCall( inLog( PrepareStoreCopyRequestHandler.class ).warn( containsString( "Unable to prepare for store copy" ) ) );

        // and the protocol is reset to expect any message type after listing has been transmitted
        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    private PrepareStoreCopyRequestHandler createHandler()
    {
        catchupServerProtocol = new CatchupServerProtocol();
        catchupServerProtocol.expect( CatchupServerProtocol.State.PREPARE_STORE_COPY );
        when( db.getStoreId() ).thenReturn( new org.neo4j.storageengine.api.StoreId( 1, 2, 5, 3, 4 ) );

        PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider = mock( PrepareStoreCopyFilesProvider.class );
        when( prepareStoreCopyFilesProvider.prepareStoreCopyFiles( any() ) ).thenReturn( prepareStoreCopyFiles );

        return new PrepareStoreCopyRequestHandler( catchupServerProtocol, db, prepareStoreCopyFilesProvider, logProvider );
    }

    private void configureProvidedStoreCopyFiles( StoreResource[] atomicFiles, File[] files, LongSet indexIds, long lastCommitedTx )
            throws IOException
    {
        when( prepareStoreCopyFiles.getAtomicFilesSnapshot() ).thenReturn( atomicFiles );
        when( prepareStoreCopyFiles.getNonAtomicIndexIds() ).thenReturn( indexIds );
        when( prepareStoreCopyFiles.listReplayableFiles() ).thenReturn( files );
        when( checkPointer.lastCheckPointedTransactionId() ).thenReturn( lastCommitedTx );
    }
}
