/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3;

import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ServerInboundRequestsLogger;
import com.neo4j.causalclustering.catchup.RequestDecoderDispatcher;
import com.neo4j.causalclustering.catchup.RequestMessageTypeEncoder;
import com.neo4j.causalclustering.catchup.ResponseMessageTypeEncoder;
import com.neo4j.causalclustering.catchup.ServerMessageTypeHandler;
import com.neo4j.causalclustering.catchup.storecopy.FileChunkEncoder;
import com.neo4j.causalclustering.catchup.storecopy.FileHeaderEncoder;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseEncoder;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdRequestDecoder;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponseEncoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.CatchupErrorResponseEncoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.CoreSnapshotRequestDecoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequestDecoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequestDecoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdResponseEncoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequestDecoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.StoreCopyFinishedResponseEncoder;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequestDecoder;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullResponseEncoder;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotEncoder;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import com.neo4j.causalclustering.protocol.ServerNettyPipelineBuilder;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CatchupProtocolServerInstallerV3 implements ProtocolInstaller<Orientation.Server>
{
    private static final ApplicationProtocols APPLICATION_PROTOCOL = ApplicationProtocols.CATCHUP_3_0;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,CatchupProtocolServerInstallerV3>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider, CatchupServerHandler catchupServerHandler )
        {
            super( APPLICATION_PROTOCOL,
                   modifiers -> new CatchupProtocolServerInstallerV3( pipelineBuilderFactory, modifiers, logProvider, catchupServerHandler ) );
        }
    }

    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final List<ModifierProtocolInstaller<Orientation.Server>> modifiers;
    private final Log log;

    private final LogProvider logProvider;
    private final CatchupServerHandler catchupServerHandler;

    public CatchupProtocolServerInstallerV3( NettyPipelineBuilderFactory pipelineBuilderFactory, List<ModifierProtocolInstaller<Orientation.Server>> modifiers,
                                             LogProvider logProvider, CatchupServerHandler catchupServerHandler )
    {
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.modifiers = modifiers;
        this.log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
        this.catchupServerHandler = catchupServerHandler;
    }

    /**
     * Uses latest version of handlers. Hence version naming may be less than the current version if no change was needed for that handler
     */
    @Override
    public void install( Channel channel )
    {
        CatchupServerProtocol state = new CatchupServerProtocol();

        final var builder = pipelineBuilderFactory
                .server( channel, log )
                .modify( modifiers )
                .addFraming();

        encoders( builder, state )
                .add( "in_req_type", serverMessageHandler( state ) )
                .add( "dec_req_dispatch", requestDecoders( state ) )
                .add( "out_chunked_write", new ChunkedWriteHandler() );
        handlers( builder, state ).install();
    }

    protected ServerNettyPipelineBuilder encoders( ServerNettyPipelineBuilder builder, CatchupServerProtocol state )
    {
        return builder
                .add( "enc_req_type", new RequestMessageTypeEncoder() )
                .add( "enc_res_type", new ResponseMessageTypeEncoder() )
                .add( "enc_res_tx_pull", new TxPullResponseEncoder() )
                .add( "enc_res_store_id", new GetStoreIdResponseEncoder() )
                .add( "enc_res_database_id", new GetDatabaseIdResponseEncoder() )
                .add( "enc_res_copy_fin", new StoreCopyFinishedResponseEncoder() )
                .add( "enc_res_tx_fin", new TxStreamFinishedResponseEncoder() )
                .add( "enc_res_pre_copy", new PrepareStoreCopyResponse.Encoder() )
                .add( "enc_snapshot", new CoreSnapshotEncoder() )
                .add( "enc_file_chunk", new FileChunkEncoder() )
                .add( "enc_file_header", new FileHeaderEncoder() )
                .add( "enc_catchup_error", new CatchupErrorResponseEncoder() );
    }

    protected ServerNettyPipelineBuilder handlers( ServerNettyPipelineBuilder builder, CatchupServerProtocol state )
    {
        return builder.add( "log_inbound_req", new ServerInboundRequestsLogger( logProvider ) )
                .add( "hnd_req_database_id", catchupServerHandler.getDatabaseIdRequestHandler( state ) )
                .add( "hnd_req_tx", catchupServerHandler.txPullRequestHandler( state ) )
                .add( "hnd_req_store_id", catchupServerHandler.getStoreIdRequestHandler( state ) )
                .add( "hnd_req_store_listing", catchupServerHandler.storeListingRequestHandler( state ) )
                .add( "hnd_req_store_file", catchupServerHandler.getStoreFileRequestHandler( state ) )
                .add( "hnd_req_snapshot", catchupServerHandler.snapshotHandler( state ) );
    }

    private ChannelHandler serverMessageHandler( CatchupServerProtocol state )
    {
        return new ServerMessageTypeHandler( state, logProvider );
    }

    private ChannelInboundHandler requestDecoders( CatchupServerProtocol protocol )
    {
        RequestDecoderDispatcher<CatchupServerProtocol.State> decoderDispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        decoders( decoderDispatcher );

        return decoderDispatcher;
    }

    protected void decoders( RequestDecoderDispatcher<CatchupServerProtocol.State> decoderDispatcher )
    {
        decoderDispatcher.register( CatchupServerProtocol.State.GET_DATABASE_ID, new GetDatabaseIdRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.TX_PULL, new TxPullRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE_ID, new GetStoreIdRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_CORE_SNAPSHOT, new CoreSnapshotRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.PREPARE_STORE_COPY, new PrepareStoreCopyRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE_FILE, new GetStoreFileRequestDecoder() );
    }

    protected CatchupServerHandler handler()
    {
        return catchupServerHandler;
    }

    @Override
    public Collection<Collection<ModifierProtocol>> modifiers()
    {
        return modifiers.stream().map( ModifierProtocolInstaller::protocols ).collect( Collectors.toList() );
    }
}
