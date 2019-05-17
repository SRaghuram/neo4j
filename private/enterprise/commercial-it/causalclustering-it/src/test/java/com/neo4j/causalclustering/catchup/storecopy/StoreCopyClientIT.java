/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressProvider.SingleAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.helper.ConstantTimeTimeoutStrategy;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.net.Server;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.helpers.ListenSocketAddress;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipThreadLeakageGuard;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@SkipThreadLeakageGuard
@ExtendWith( {SuppressOutputExtension.class, DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class StoreCopyClientIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private final AssertableLogProvider assertableLogProvider = new AssertableLogProvider( true );
    private final TerminationCondition defaultTerminationCondition = TerminationCondition.CONTINUE_INDEFINITELY;
    private final FakeFile fileA = new FakeFile( "fileA", "This is file a content" );
    private final FakeFile fileB = new FakeFile( "another-file-b", "Totally different content 123" );
    private final File targetLocation = new File( "copyTargetLocation" );
    private JobScheduler scheduler;
    private LogProvider logProvider;
    private StoreCopyClient subject;
    private Server catchupServer;
    private FakeCatchupServer serverHandler;

    private static void writeContents( FileSystemAbstraction fileSystemAbstraction, File file, String contents )
    {
        byte[] bytes = contents.getBytes();
        try ( StoreChannel storeChannel = fileSystemAbstraction.write( file ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @BeforeEach
    void setup() throws Throwable
    {
        scheduler = new ThreadPoolJobScheduler();
        logProvider = new DuplicatingLogProvider( assertableLogProvider, FormattedLogProvider.withDefaultLogLevel( Level.DEBUG ).toOutputStream( System.out ) );
        serverHandler = new FakeCatchupServer( logProvider, testDirectory, fs );
        serverHandler.addFile( fileA );
        serverHandler.addFile( fileB );
        writeContents( fs, relative( fileA.getFilename() ), fileA.getContent() );
        writeContents( fs, relative( fileB.getFilename() ), fileB.getContent() );

        ListenSocketAddress listenAddress = new ListenSocketAddress( "localhost", PortAuthority.allocatePort() );
        catchupServer = CausalClusteringTestHelpers.getCatchupServer( serverHandler, listenAddress, scheduler );
        catchupServer.start();

        CatchupClientFactory catchUpClient = CausalClusteringTestHelpers.getCatchupClient( logProvider, scheduler );
        catchUpClient.start();

        ConstantTimeTimeoutStrategy storeCopyBackoffStrategy = new ConstantTimeTimeoutStrategy( 1, TimeUnit.MILLISECONDS );

        subject = new StoreCopyClient( catchUpClient, new DatabaseId( DEFAULT_DATABASE_NAME ), Monitors::new, logProvider, storeCopyBackoffStrategy );
    }

    @AfterEach
    void shutdown() throws Throwable
    {
        catchupServer.stop();
        scheduler.shutdown();
    }

    @Test
    void canPerformCatchup() throws StoreCopyFailedException, IOException
    {
        // given local client has a store
        InMemoryStoreStreamProvider storeFileStream = new InMemoryStoreStreamProvider();

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = new SingleAddressProvider( from( catchupServer.address().getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, serverHandler.getStoreId(), storeFileStream, () -> defaultTerminationCondition, targetLocation );

        // then the catchup is successful
        Set<String> expectedFiles = new HashSet<>( Arrays.asList( fileA.getFilename(), fileB.getFilename() ) );
        assertEquals( expectedFiles, storeFileStream.fileStreams().keySet() );
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( storeFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( storeFileStream, fileB.getFilename() ) );
    }

    @Test
    void shouldHandleMultipleRequestsOnReusedChannel()
    {
        CatchupAddressProvider catchupAddressProvider = new SingleAddressProvider( from( catchupServer.address().getPort() ) );
        for ( int i = 0; i < 100; i++ )
        {
            StoreFileStreamProvider storeFileStream = new IgnoringStoreFileStreamProvider();
            assertDoesNotThrow(
                    () -> subject.copyStoreFiles( catchupAddressProvider, serverHandler.getStoreId(), storeFileStream, () -> defaultTerminationCondition,
                            targetLocation ) );
        }
    }

    @Test
    void failedFileCopyShouldRetry() throws StoreCopyFailedException, IOException
    {
        // given a file will fail twice before succeeding
        fileB.setRemainingFailed( 2 );

        // and remote node has a store
        // and local client has a store
        InMemoryStoreStreamProvider clientStoreFileStream = new InMemoryStoreStreamProvider();

        // when catchup is performed for valid transactionId and StoreId
        CatchupAddressProvider catchupAddressProvider = new SingleAddressProvider( from( catchupServer.address().getPort() ) );
        subject.copyStoreFiles( catchupAddressProvider, serverHandler.getStoreId(), clientStoreFileStream, () -> defaultTerminationCondition, targetLocation );

        // then the catchup is successful
        Set<String> expectedFiles = new HashSet<>( Arrays.asList( fileA.getFilename(), fileB.getFilename() ) );
        assertEquals( expectedFiles, clientStoreFileStream.fileStreams().keySet() );

        // and
        assertEquals( fileContent( relative( fileA.getFilename() ) ), clientFileContents( clientStoreFileStream, fileA.getFilename() ) );
        assertEquals( fileContent( relative( fileB.getFilename() ) ), clientFileContents( clientStoreFileStream, fileB.getFilename() ) );

        // and verify server had exactly 2 failed calls before having a 3rd succeeding request
        assertEquals( 3, serverHandler.getRequestCount( fileB.getFilename() ) );

        // and verify server had exactly 1 call for all other files
        assertEquals( 1, serverHandler.getRequestCount( fileA.getFilename() ) );
    }

    @Test
    void shouldNotAppendToFileWhenRetryingWithNewFile() throws Throwable
    {
        // given
        String fileName = "foo";
        String copyFileName = "bar";
        String unfinishedContent = "abcd";
        String finishedContent = "abcdefgh";
        Iterator<String> contents = Iterators.iterator( unfinishedContent, finishedContent );

        // and
        FakeCatchupServer halfWayFailingServerHandler = new FakeCatchupServer( logProvider, testDirectory, fs )
        {
            @Override
            public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol )
            {
                return new SimpleChannelInboundHandler<GetStoreFileRequest>()
                {
                    @Override
                    protected void channelRead0( ChannelHandlerContext ctx, GetStoreFileRequest msg )
                    {
                        // create the files and write the given content
                        File file = new File( fileName );
                        File fileCopy = new File( copyFileName );
                        String thisContent = contents.next();
                        writeContents( fs, file, thisContent );
                        writeContents( fs, fileCopy, thisContent );

                        sendFile( ctx, file );
                        sendFile( ctx, fileCopy );
                        StoreCopyFinishedResponse.Status status =
                                contents.hasNext() ? StoreCopyFinishedResponse.Status.E_UNKNOWN : StoreCopyFinishedResponse.Status.SUCCESS;
                        new StoreFileStreamingProtocol().end( ctx, status, -1 );
                        catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                    }

                    private void sendFile( ChannelHandlerContext ctx, File file )
                    {
                        ctx.write( ResponseMessageType.FILE );
                        ctx.write( new FileHeader( file.getName() ) );
                        ctx.writeAndFlush( new FileSender( new StoreResource( file, file.getName(), 16, fs ) ) ).addListener(
                                future -> fs.deleteFile( file ) );
                    }
                };
            }

            @Override
            public ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol )
            {
                return new SimpleChannelInboundHandler<PrepareStoreCopyRequest>()
                {
                    @Override
                    protected void channelRead0( ChannelHandlerContext ctx, PrepareStoreCopyRequest msg )
                    {
                        ctx.write( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                        ctx.writeAndFlush( PrepareStoreCopyResponse.success( new File[]{new File( fileName )}, 1 ) );
                        catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                    }
                };
            }
        };

        Server halfWayFailingServer = null;

        try
        {
            // when
            ListenSocketAddress listenAddress = new ListenSocketAddress( "localhost", PortAuthority.allocatePort() );
            halfWayFailingServer = CausalClusteringTestHelpers.getCatchupServer( halfWayFailingServerHandler, listenAddress, scheduler );
            halfWayFailingServer.start();

            CatchupAddressProvider addressProvider =
                    new SingleAddressProvider( new AdvertisedSocketAddress( listenAddress.getHostname(), listenAddress.getPort() ) );

            StoreId storeId = halfWayFailingServerHandler.getStoreId();
            File databaseDir = testDirectory.storeDir();
            StreamToDiskProvider streamToDiskProvider = new StreamToDiskProvider( databaseDir, fs, new Monitors() );

            // and
            subject.copyStoreFiles( addressProvider, storeId, streamToDiskProvider, () -> defaultTerminationCondition, targetLocation );

            // then
            assertEquals( fileContent( new File( databaseDir, fileName ) ), finishedContent );

            // and
            File fileCopy = new File( databaseDir, copyFileName );

            ByteBuffer buffer = ByteBuffer.wrap( new byte[finishedContent.length()] );
            try ( StoreChannel storeChannel = fs.write( fileCopy ) )
            {
                storeChannel.read( buffer );
            }
            assertEquals( finishedContent, new String( buffer.array(), StandardCharsets.UTF_8 ) );
        }
        finally
        {
            halfWayFailingServer.stop();
            halfWayFailingServer.shutdown();
        }
    }

    @Test
    void shouldLogConnectionRefusedMessage()
    {
        InMemoryStoreStreamProvider clientStoreFileStream = new InMemoryStoreStreamProvider();
        int port = PortAuthority.allocatePort();

        CatchupAddressProvider addressProvider = new CatchupAddressProvider()
        {
            @Override
            public AdvertisedSocketAddress primary( DatabaseId databaseId )
            {
                return from( catchupServer.address().getPort() );
            }

            @Override
            public AdvertisedSocketAddress secondary( DatabaseId databaseId )
            {

                return new AdvertisedSocketAddress( "localhost", port );
            }
        };

        assertThrows( StoreCopyFailedException.class, () ->
                subject.copyStoreFiles( addressProvider, serverHandler.getStoreId(), clientStoreFileStream, Once::new, targetLocation ) );

        assertableLogProvider.containsMatchingLogCall( inLog( StoreCopyClient.class )
                .warn( any( String.class ), equalTo( "Connection refused: localhost/127.0.0.1:" + port ) ) );
    }

    @Test
    void shouldLogUpstreamIssueMessage()
    {
        InMemoryStoreStreamProvider clientStoreFileStream = new InMemoryStoreStreamProvider();
        CatchupAddressResolutionException catchupAddressResolutionException = new CatchupAddressResolutionException( new MemberId( UUID.randomUUID() ) );

        CatchupAddressProvider addressProvider = new CatchupAddressProvider()
        {
            @Override
            public AdvertisedSocketAddress primary( DatabaseId databaseId )
            {
                return from( catchupServer.address().getPort() );
            }

            @Override
            public AdvertisedSocketAddress secondary( DatabaseId databaseId ) throws CatchupAddressResolutionException
            {
                throw catchupAddressResolutionException;
            }
        };

        assertThrows( StoreCopyFailedException.class, () ->
                subject.copyStoreFiles( addressProvider, serverHandler.getStoreId(), clientStoreFileStream, Once::new, targetLocation ) );

        assertableLogProvider.assertContainsExactlyOneMessageMatching( startsWith( "Unable to resolve address for" ) );
        assertableLogProvider.assertLogStringContains( catchupAddressResolutionException.getMessage() );
    }

    private File relative( String filename )
    {
        return testDirectory.file( filename );
    }

    private String fileContent( File file ) throws IOException
    {
        return CausalClusteringTestHelpers.fileContent( file, fs );
    }

    private static AdvertisedSocketAddress from( int port )
    {
        return new AdvertisedSocketAddress( "localhost", port );
    }

    private static String clientFileContents( InMemoryStoreStreamProvider storeFileStreamsProvider, String filename )
    {
        return storeFileStreamsProvider.fileStreams().get( filename ).toString();
    }

    private static class Once implements TerminationCondition
    {
        @Override
        public void assertContinue() throws StoreCopyFailedException
        {
            throw new StoreCopyFailedException( "One try only" );
        }
    }

    private static class IgnoringStoreFileStreamProvider implements StoreFileStreamProvider
    {
        @Override
        public StoreFileStream acquire( String destination, int requiredAlignment )
        {
            return new StoreFileStream()
            {
                @Override
                public void write( byte[] data )
                {
                    // ignore
                }

                @Override
                public void close()
                {
                    // ignore
                }
            };
        }
    }
}
