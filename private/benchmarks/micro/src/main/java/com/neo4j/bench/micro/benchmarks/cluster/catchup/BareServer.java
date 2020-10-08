/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsRequest;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static com.neo4j.causalclustering.catchup.CatchupServerProtocol.State.MESSAGE_TYPE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.ALL_DATABASE_IDS_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.CORE_SNAPSHOT;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.DATABASE_ID_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.INFO_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.PREPARE_STORE_COPY_RESPONSE;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.STORE_ID;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.TX;
import static com.neo4j.causalclustering.catchup.ResponseMessageType.TX_STREAM_FINISHED;

class BareServer implements CatchupServerHandler
{
    private static final String ATOMIC = "atomic.bin";

    private final Log log;
    private final BareFilesHolder fileHolder;
    private final BareTransactionProvider transactionProvider;
    private DatabaseId databaseId;
    private StoreId storeId;
    private long lastTxId;

    BareServer( LogProvider logProvider, BareFilesHolder fileHolder, BareTransactionProvider transactionProvider ) throws IOException
    {
        this.log = logProvider.getLog( getClass() );
        this.fileHolder = fileHolder;
        this.transactionProvider = transactionProvider;

        var rnd = new Random();
        databaseId = DatabaseIdFactory.from( UUID.randomUUID() );
        storeId = new StoreId( rnd.nextInt( 1000 ), rnd.nextInt( 1000 ) + 1000, rnd.nextInt( 1000 ) + 2000 );
        lastTxId = rnd.nextInt( 1000 ) + 3000;

        fileHolder.prepareFile( ATOMIC, 1024 );
    }

    @Override
    public ChannelHandler getDatabaseIdRequestHandler( CatchupServerProtocol protocol )
    {
        return new SimpleChannelInboundHandler<GetDatabaseIdRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, GetDatabaseIdRequest msg )
            {
                respond( ctx, protocol, DATABASE_ID_RESPONSE, databaseId );
            }
        };
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol protocol )
    {
        return new SimpleChannelInboundHandler<GetStoreIdRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, GetStoreIdRequest msg )
            {
                respond( ctx, protocol, STORE_ID, storeId );
            }
        };
    }

    @Override
    public ChannelHandler storeListingRequestHandler( CatchupServerProtocol protocol )
    {
        return new SimpleChannelInboundHandler<PrepareStoreCopyRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, PrepareStoreCopyRequest msg )
            {
                fileHolder.sendFile( ctx, ATOMIC );
                var nonAtomics =
                        Stream.of( fileHolder.getFiles() ).filter( file -> !file.getFileName().toString().equals( ATOMIC ) ).toArray( Path[]::new );
                respond( ctx, protocol, PREPARE_STORE_COPY_RESPONSE, PrepareStoreCopyResponse.success( nonAtomics, lastTxId ) );
            }
        };
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol protocol )
    {
        return new SimpleChannelInboundHandler<GetStoreFileRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, GetStoreFileRequest msg )
            {
                fileHolder.sendFileWithFileComplete( ctx, msg.path().getFileName().toString(), lastTxId );
                protocol.expect( MESSAGE_TYPE );
            }
        };
    }

    @Override
    public ChannelHandler txPullRequestHandler( CatchupServerProtocol protocol )
    {
        return new SimpleChannelInboundHandler<TxPullRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, TxPullRequest msg )
            {
                if ( transactionProvider.reset() )
                {
                    ctx.write( TX );
                    while ( transactionProvider.hasNext() )
                    {
                        ctx.write( new TxPullResponse( storeId, transactionProvider.next() ) );
                    }
                    ctx.write( TxPullResponse.EMPTY );
                }
                respond( ctx, protocol, TX_STREAM_FINISHED, new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, lastTxId ) );
            }
        };
    }

    @Override
    public ChannelHandler snapshotHandler( CatchupServerProtocol protocol )
    {
        return new SimpleChannelInboundHandler<CoreSnapshotRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, CoreSnapshotRequest msg )
            {
                respond( ctx, protocol, CORE_SNAPSHOT, new CoreSnapshot( lastTxId, 1L ) );
            }
        };
    }

    @Override
    public ChannelHandler getAllDatabaseIds( CatchupServerProtocol protocol )
    {
        return new SimpleChannelInboundHandler<GetAllDatabaseIdsRequest>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, GetAllDatabaseIdsRequest msg )
            {
                respond( ctx, protocol, ALL_DATABASE_IDS_RESPONSE, storeId );
            }
        };
    }

    @Override
    public ChannelHandler getInfo( CatchupServerProtocol protocol )
    {
        return new SimpleChannelInboundHandler<InfoResponse>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, InfoResponse msg )
            {
                respond( ctx, protocol, INFO_RESPONSE, InfoResponse.create( 1L, null ) );
            }
        };
    }

    private void respond( ChannelHandlerContext ctx, CatchupServerProtocol protocol, Object... messages )
    {
        Stream.of( messages ).forEach( ctx::write );
        ctx.flush();
        protocol.expect( MESSAGE_TYPE );
    }
}
