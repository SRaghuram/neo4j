/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.helpers.FakeJobScheduler;
import com.neo4j.causalclustering.identity.StoreId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.TriggerInfo;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

//TODO: Update tests with database name related cases.
class StoreCopyRequestHandlerTest
{
    private static final StoreId STORE_ID_MISMATCHING = new StoreId( 1, 1, 1, 1 );
    private static final StoreId STORE_ID_MATCHING = new StoreId( 1, 2, 3, 4 );
    private final DefaultFileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();

    private final Database database = mock( Database.class );
    private final FakeCheckPointer checkPointer = new FakeCheckPointer();
    private EmbeddedChannel embeddedChannel;
    private CatchupServerProtocol catchupServerProtocol;
    private JobScheduler jobScheduler = new FakeJobScheduler();

    @BeforeEach
    void setup()
    {
        catchupServerProtocol = new CatchupServerProtocol();
        catchupServerProtocol.expect( CatchupServerProtocol.State.GET_STORE_FILE );
        StoreCopyRequestHandler storeCopyRequestHandler =
                new NiceStoreCopyRequestHandler( catchupServerProtocol, () -> database, new StoreFileStreamingProtocol(),
                        fileSystemAbstraction, NullLogProvider.getInstance() );
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependency( checkPointer );
        when( database.getStoreId() ).thenReturn( new org.neo4j.storageengine.api.StoreId( 1, 2, 5, 3, 4 ) );
        when( database.getDependencyResolver() ).thenReturn( dependencies );
        when( database.getDatabaseLayout() ).thenReturn( DatabaseLayout.of( new File( "." ) ) );
        when( database.getScheduler() ).thenReturn( jobScheduler );
        embeddedChannel = new EmbeddedChannel( storeCopyRequestHandler );
    }

    @Test
    void shouldGiveProperErrorOnStoreIdMismatch()
    {
        embeddedChannel.writeInbound( new GetStoreFileRequest( STORE_ID_MISMATCHING,
                new File( "some-file" ), 1, DEFAULT_DATABASE_NAME ) );

        assertEquals( ResponseMessageType.STORE_COPY_FINISHED, embeddedChannel.readOutbound() );
        StoreCopyFinishedResponse expectedResponse = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH );
        assertEquals( expectedResponse, embeddedChannel.readOutbound() );

        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    void shouldGiveProperErrorOnTxBehind()
    {
        embeddedChannel.writeInbound( new GetStoreFileRequest( STORE_ID_MATCHING,
                new File( "some-file" ), 2, DEFAULT_DATABASE_NAME ) );

        assertEquals( ResponseMessageType.STORE_COPY_FINISHED, embeddedChannel.readOutbound() );
        StoreCopyFinishedResponse expectedResponse = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND );
        assertEquals( expectedResponse, embeddedChannel.readOutbound() );

        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    void shouldResetProtocolAndGiveErrorOnUncheckedException()
    {
        when( database.getStoreId() ).thenThrow( new IllegalStateException() );

        assertThrows( IllegalStateException.class,
                () -> embeddedChannel.writeInbound( new GetStoreFileRequest( STORE_ID_MATCHING, new File( "some-file" ), 1, DEFAULT_DATABASE_NAME ) ) );

        assertEquals( ResponseMessageType.STORE_COPY_FINISHED, embeddedChannel.readOutbound() );
        StoreCopyFinishedResponse expectedResponse = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_UNKNOWN );
        assertEquals( expectedResponse, embeddedChannel.readOutbound() );

        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    void shoulResetProtocolAndGiveErrorIfFilesThrowException()
    {
        EmbeddedChannel alternativeChannel = new EmbeddedChannel(
                new EvilStoreCopyRequestHandler( catchupServerProtocol, () -> database, new StoreFileStreamingProtocol(),
                        fileSystemAbstraction, NullLogProvider.getInstance() ) );

        assertThrows( IllegalStateException.class,
                () -> alternativeChannel.writeInbound( new GetStoreFileRequest( STORE_ID_MATCHING, new File( "some-file" ), 1, DEFAULT_DATABASE_NAME ) ) );

        assertEquals( ResponseMessageType.STORE_COPY_FINISHED, alternativeChannel.readOutbound() );
        StoreCopyFinishedResponse expectedResponse = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.E_UNKNOWN );
        assertEquals( expectedResponse, alternativeChannel.readOutbound() );

        assertTrue( catchupServerProtocol.isExpecting( CatchupServerProtocol.State.MESSAGE_TYPE ) );
    }

    @Test
    void transactionsTooFarBehindStartCheckpointAsynchronously()
    {
        // given checkpoint will fail if performed
        checkPointer._tryCheckPoint = Optional.empty();

        // when
        RuntimeException error = assertThrows( RuntimeException.class,
                () -> embeddedChannel.writeInbound( new GetStoreFileRequest( STORE_ID_MATCHING, new File( "some-file" ), 123, DEFAULT_DATABASE_NAME ) ) );

        assertEquals( "FakeCheckPointer", error.getMessage() );

        // then should have received error message
        assertEquals( ResponseMessageType.STORE_COPY_FINISHED, embeddedChannel.readOutbound() );

        // and should have failed on async
        assertEquals( 1, checkPointer.invocationCounter.get() );
        assertEquals( 1, checkPointer.failCounter.get() );
    }

    private class NiceStoreCopyRequestHandler extends StoreCopyRequestHandler<GetStoreFileRequest>
    {
        private NiceStoreCopyRequestHandler( CatchupServerProtocol protocol, Supplier<Database> dataSource,
                StoreFileStreamingProtocol storeFileStreamingProtocol,
                FileSystemAbstraction fs, LogProvider logProvider )
        {
            super( protocol, dataSource, storeFileStreamingProtocol, fs, logProvider, GetStoreFileRequest.class );
        }

        @Override
        ResourceIterator<StoreFileMetadata> files( GetStoreFileRequest request, Database database )
        {
            return Iterators.emptyResourceIterator();
        }
    }

    private class EvilStoreCopyRequestHandler extends StoreCopyRequestHandler<GetStoreFileRequest>
    {
        private EvilStoreCopyRequestHandler( CatchupServerProtocol protocol, Supplier<Database> dataSource,
                StoreFileStreamingProtocol storeFileStreamingProtocol, FileSystemAbstraction fs, LogProvider logProvider )
        {
            super( protocol, dataSource, storeFileStreamingProtocol, fs, logProvider, GetStoreFileRequest.class );
        }

        @Override
        ResourceIterator<StoreFileMetadata> files( GetStoreFileRequest request, Database database )
        {
            throw new IllegalStateException( "I am evil" );
        }
    }

    private class FakeCheckPointer implements CheckPointer
    {
        Optional<Long> _checkPointIfNeeded = Optional.of( 1L );
        Optional<Long> _tryCheckPoint = Optional.of( 1L );
        Optional<Long> _forceCheckPoint = Optional.of( 1L );
        Optional<Long> _lastCheckPointedTransactionId = Optional.of( 1L );
        Supplier<RuntimeException> exceptionIfEmpty = () -> new RuntimeException( "FakeCheckPointer" );
        AtomicInteger invocationCounter = new AtomicInteger();
        AtomicInteger failCounter = new AtomicInteger();

        @Override
        public long checkPointIfNeeded( TriggerInfo triggerInfo )
        {
            incrementInvocationCounter( _checkPointIfNeeded );
            return _checkPointIfNeeded.orElseThrow( exceptionIfEmpty );
        }

        @Override
        public long tryCheckPoint( TriggerInfo triggerInfo )
        {
            incrementInvocationCounter( _tryCheckPoint );
            return _tryCheckPoint.orElseThrow( exceptionIfEmpty );
        }

        @Override
        public long tryCheckPointNoWait( TriggerInfo triggerInfo )
        {
            incrementInvocationCounter( _tryCheckPoint );
            return _tryCheckPoint.orElseThrow( exceptionIfEmpty );
        }

        @Override
        public long forceCheckPoint( TriggerInfo triggerInfo )
        {
            incrementInvocationCounter( _forceCheckPoint );
            return _forceCheckPoint.orElseThrow( exceptionIfEmpty );
        }

        @Override
        public long lastCheckPointedTransactionId()
        {
            incrementInvocationCounter( _lastCheckPointedTransactionId );
            return _lastCheckPointedTransactionId.orElseThrow( exceptionIfEmpty );
        }

        private void incrementInvocationCounter( Optional<Long> variable )
        {
            if ( variable.isPresent() )
            {
                invocationCounter.getAndIncrement();
                return;
            }
            failCounter.getAndIncrement();
        }
    }
}
