/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.rule.TestDirectory;

class FakeCatchupServer implements CatchupServerHandler
{
    private final Set<FakeFile> filesystem = new HashSet<>();
    private final Map<String,Integer> pathToRequestCountMapping = new HashMap<>();
    private final Log log;
    private TestDirectory testDirectory;
    private FileSystemAbstraction fileSystemAbstraction;
    private long startTxId;
    private int maxChunkSize = 32768;

    FakeCatchupServer( LogProvider logProvider, TestDirectory testDirectory, FileSystemAbstraction fileSystemAbstraction )
    {
        log = logProvider.getLog( FakeCatchupServer.class );
        this.testDirectory = testDirectory;
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    void addFile( FakeFile fakeFile )
    {
        filesystem.add( fakeFile );
    }

    int getRequestCount( String file )
    {
        return pathToRequestCountMapping.getOrDefault( file, 0 );
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new SimpleChannelInboundHandler<GetStoreFileRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext channelHandlerContext, GetStoreFileRequest getStoreFileRequest )
            {
                log.info( "Received request for file %s", getStoreFileRequest.path().getFileName() );
                incrementRequestCount( getStoreFileRequest.path() );
                try
                {
                    if ( handleFileDoesNotExist( channelHandlerContext, getStoreFileRequest ) )
                    {
                        catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                        return;
                    }
                    handleFileExists( channelHandlerContext, getStoreFileRequest.path() );
                }
                finally
                {
                    catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
                }
            }
        };
    }

    private boolean handleFileDoesNotExist( ChannelHandlerContext channelHandlerContext, GetStoreFileRequest getStoreFileRequest )
    {
        FakeFile file = findFile( filesystem, getStoreFileRequest.path().getFileName().toString() );
        if ( file.getRemainingFailed() > 0 )
        {
            file.setRemainingFailed( file.getRemainingFailed() - 1 );
            log.info( "FakeServer failing for file %s", getStoreFileRequest.path() );
            failed( channelHandlerContext );
            return true;
        }
        return false;
    }

    private void failed( ChannelHandlerContext channelHandlerContext )
    {
        new StoreFileStreamingProtocol( maxChunkSize ).end( channelHandlerContext, StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND, -1 );
    }

    private FakeFile findFile( Set<FakeFile> filesystem, String filename )
    {
        return filesystem.stream()
                .filter( fakeFile -> filename.equals( fakeFile.getFilename() ) )
                .findFirst()
                .orElseThrow( () -> new RuntimeException( "FakeFile should handle all cases with regards to how server should respond" ) );
    }

    private void handleFileExists( ChannelHandlerContext channelHandlerContext, Path file )
    {
        log.info( "FakeServer File %s does exist", file );
        channelHandlerContext.writeAndFlush( ResponseMessageType.FILE );
        channelHandlerContext.writeAndFlush( new FileHeader( file.getFileName().toString() ) );
        StoreResource storeResource = storeResourceFromEntry( file );
        channelHandlerContext.writeAndFlush( new FileSender( storeResource, 32768 ) );
        new StoreFileStreamingProtocol( maxChunkSize ).end( channelHandlerContext, StoreCopyFinishedResponse.Status.SUCCESS, startTxId );
    }

    private void incrementRequestCount( Path file )
    {
        String path = file.getFileName().toString();
        int count = pathToRequestCountMapping.getOrDefault( path, 0 );
        pathToRequestCountMapping.put( path, count + 1 );
    }

    private StoreResource storeResourceFromEntry( Path file )
    {
        file = testDirectory.filePath( file.getFileName().toString() );
        return new StoreResource( file, file.toAbsolutePath().toString(), 16, fileSystemAbstraction );
    }

    @Override
    public ChannelHandler getDatabaseIdRequestHandler( CatchupServerProtocol protocol )
    {
        return new ChannelInboundHandlerAdapter();
    }

    @Override
    public ChannelHandler txPullRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new ChannelInboundHandlerAdapter();
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new ChannelInboundHandlerAdapter();
    }

    @Override
    public ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new SimpleChannelInboundHandler<PrepareStoreCopyRequest>()
        {

            @Override
            protected void channelRead0( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyRequest prepareStoreCopyRequest )
            {
                channelHandlerContext.writeAndFlush( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                List<Path> list = filesystem.stream().map( FakeFile::getFile ).map( File::toPath ).collect( Collectors.toList() );
                Path[] files = new Path[list.size()];
                files = list.toArray( files );
                startTxId = 123L;
                channelHandlerContext.writeAndFlush( PrepareStoreCopyResponse.success( files, startTxId ) );
                catchupServerProtocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
            }
        };
    }

    @Override
    public ChannelHandler snapshotHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new ChannelInboundHandlerAdapter();
    }

    public StoreId getStoreId()
    {
        return new StoreId( 1, 2, 3, 4, 5 );
    }
}
