/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v2;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.RequestDecoderDispatcher;
import com.neo4j.causalclustering.catchup.RequestMessageTypeEncoder;
import com.neo4j.causalclustering.catchup.ResponseMessageTypeEncoder;
import com.neo4j.causalclustering.catchup.ServerMessageTypeHandler;
import com.neo4j.causalclustering.catchup.SimpleRequestDecoder;
import com.neo4j.causalclustering.catchup.storecopy.FileChunkEncoder;
import com.neo4j.causalclustering.catchup.storecopy.FileHeaderEncoder;
import com.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseEncoder;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseEncoder;
import com.neo4j.causalclustering.catchup.tx.TxPullResponseEncoder;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseEncoder;
import com.neo4j.causalclustering.catchup.v2.storecopy.CatchupErrorResponseEncoder;
import com.neo4j.causalclustering.catchup.v2.storecopy.GetIndexFilesRequestMarshalV2;
import com.neo4j.causalclustering.catchup.v2.storecopy.GetStoreFileRequestMarshalV2;
import com.neo4j.causalclustering.catchup.v2.storecopy.GetStoreIdRequestDecoderV2;
import com.neo4j.causalclustering.catchup.v2.storecopy.PrepareStoreCopyRequestDecoderV2;
import com.neo4j.causalclustering.catchup.v2.tx.TxPullRequestDecoderV2;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotEncoder;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;

public class CatchupProtocolServerInstallerV2 implements ProtocolInstaller<Orientation.Server>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.CATCHUP_2;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,CatchupProtocolServerInstallerV2>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider, CatchupServerHandler catchupServerHandler )
        {
            super( APPLICATION_PROTOCOL,
                    modifiers -> new CatchupProtocolServerInstallerV2( pipelineBuilderFactory, modifiers, logProvider, catchupServerHandler ) );
        }
    }

    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final List<ModifierProtocolInstaller<Orientation.Server>> modifiers;
    private final Log log;

    private final LogProvider logProvider;
    private final CatchupServerHandler catchupServerHandler;

    public CatchupProtocolServerInstallerV2( NettyPipelineBuilderFactory pipelineBuilderFactory, List<ModifierProtocolInstaller<Orientation.Server>> modifiers,
            LogProvider logProvider, CatchupServerHandler catchupServerHandler )
    {
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.modifiers = modifiers;
        this.log = logProvider.getLog( getClass() );
        this.logProvider = logProvider;
        this.catchupServerHandler = catchupServerHandler;
    }

    @Override
    public void install( Channel channel ) throws Exception
    {
        CatchupServerProtocol state = new CatchupServerProtocol();

        pipelineBuilderFactory.server( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "enc_req_type", new RequestMessageTypeEncoder() )
                .add( "enc_res_type", new ResponseMessageTypeEncoder() )
                .add( "enc_res_tx_pull", new TxPullResponseEncoder() )
                .add( "enc_res_store_id", new GetStoreIdResponseEncoder() )
                .add( "enc_res_copy_fin", new StoreCopyFinishedResponseEncoder() )
                .add( "enc_res_tx_fin", new TxStreamFinishedResponseEncoder() )
                .add( "enc_res_pre_copy", new PrepareStoreCopyResponse.Encoder() )
                .add( "enc_snapshot", new CoreSnapshotEncoder() )
                .add( "enc_file_chunk", new FileChunkEncoder() )
                .add( "enc_file_header", new FileHeaderEncoder() )
                .add( "enc_catchup_error", new CatchupErrorResponseEncoder() )
                .add( "in_req_type", serverMessageHandler( state ) )
                .add( "dec_req_dispatch", requestDecoders( state ) )
                .add( "out_chunked_write", new ChunkedWriteHandler() )
                .add( "hnd_req_tx", catchupServerHandler.txPullRequestHandler( state ) )
                .add( "hnd_req_store_id", catchupServerHandler.getStoreIdRequestHandler( state ) )
                .add( "hnd_req_store_listing", catchupServerHandler.storeListingRequestHandler( state ) )
                .add( "hnd_req_store_file", catchupServerHandler.getStoreFileRequestHandler( state ) )
                .add( "hnd_req_index_snapshot", catchupServerHandler.getIndexSnapshotRequestHandler( state ) )
                .add( "hnd_req_snapshot", catchupServerHandler.snapshotHandler( state ).map( Collections::singletonList ).orElse( emptyList() ) )
                .install();
    }

    private ChannelHandler serverMessageHandler( CatchupServerProtocol state )
    {
        return new ServerMessageTypeHandler( state, logProvider );
    }

    private ChannelInboundHandler requestDecoders( CatchupServerProtocol protocol )
    {
        RequestDecoderDispatcher<CatchupServerProtocol.State> decoderDispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( CatchupServerProtocol.State.TX_PULL, new TxPullRequestDecoderV2() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE_ID, new GetStoreIdRequestDecoderV2() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_CORE_SNAPSHOT, new SimpleRequestDecoder( CoreSnapshotRequest::new ) );
        decoderDispatcher.register( CatchupServerProtocol.State.PREPARE_STORE_COPY, new PrepareStoreCopyRequestDecoderV2() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_STORE_FILE, new GetStoreFileRequestMarshalV2.Decoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_INDEX_SNAPSHOT, new GetIndexFilesRequestMarshalV2.Decoder() );
        return decoderDispatcher;
    }

    @Override
    public Protocol.ApplicationProtocol applicationProtocol()
    {
        return APPLICATION_PROTOCOL;
    }

    @Override
    public Collection<Collection<Protocol.ModifierProtocol>> modifiers()
    {
        return modifiers.stream()
                .map( ModifierProtocolInstaller::protocols )
                .collect( Collectors.toList() );
    }
}
