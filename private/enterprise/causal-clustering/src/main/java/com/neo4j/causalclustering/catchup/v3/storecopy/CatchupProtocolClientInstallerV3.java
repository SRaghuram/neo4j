/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import com.neo4j.causalclustering.catchup.ClientMessageTypeHandler;
import com.neo4j.causalclustering.catchup.RequestDecoderDispatcher;
import com.neo4j.causalclustering.catchup.RequestMessageTypeEncoder;
import com.neo4j.causalclustering.catchup.ResponseMessageTypeEncoder;
import com.neo4j.causalclustering.catchup.StoreListingResponseHandler;
import com.neo4j.causalclustering.catchup.storecopy.FileChunkDecoder;
import com.neo4j.causalclustering.catchup.storecopy.FileChunkHandler;
import com.neo4j.causalclustering.catchup.storecopy.FileHeaderDecoder;
import com.neo4j.causalclustering.catchup.storecopy.FileHeaderHandler;
import com.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseDecoder;
import com.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseHandler;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseHandler;
import com.neo4j.causalclustering.catchup.tx.TxPullResponseDecoder;
import com.neo4j.causalclustering.catchup.tx.TxPullResponseHandler;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseDecoder;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseHandler;
import com.neo4j.causalclustering.catchup.v2.storecopy.CatchupErrorResponseDecoder;
import com.neo4j.causalclustering.catchup.v2.storecopy.CatchupErrorResponseHandler;
import com.neo4j.causalclustering.catchup.v2.storecopy.GetIndexFilesRequestMarshalV2;
import com.neo4j.causalclustering.catchup.v2.storecopy.GetStoreFileRequestMarshalV2;
import com.neo4j.causalclustering.catchup.v2.storecopy.GetStoreIdRequestEncoderV2;
import com.neo4j.causalclustering.catchup.v2.storecopy.PrepareStoreCopyRequestEncoderV2;
import com.neo4j.causalclustering.catchup.v2.tx.TxPullRequestEncoderV2;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotDecoder;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestEncoder;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotResponseHandler;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CatchupProtocolClientInstallerV3 implements ProtocolInstaller<ProtocolInstaller.Orientation.Client>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.CATCHUP_3;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client,CatchupProtocolClientInstallerV3>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilder, LogProvider logProvider, CatchupResponseHandler handler )
        {
            super( APPLICATION_PROTOCOL, modifiers -> new CatchupProtocolClientInstallerV3( pipelineBuilder, modifiers, logProvider, handler ) );
        }
    }

    private final List<ModifierProtocolInstaller<Orientation.Client>> modifiers;
    private final LogProvider logProvider;
    private final Log log;
    private final NettyPipelineBuilderFactory pipelineBuilder;
    private final CatchupResponseHandler handler;

    public CatchupProtocolClientInstallerV3( NettyPipelineBuilderFactory pipelineBuilder, List<ModifierProtocolInstaller<Orientation.Client>> modifiers,
            LogProvider logProvider, CatchupResponseHandler handler )
    {
        this.modifiers = modifiers;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.pipelineBuilder = pipelineBuilder;
        this.handler = handler;
    }

    /**
     * Uses latest version of handlers. Hence version naming may be less than the current version if no change was needed for that handler
     */
    @Override
    public void install( Channel channel ) throws Exception
    {
        CatchupClientProtocol protocol = new CatchupClientProtocol();

        RequestDecoderDispatcher<CatchupClientProtocol.State> decoderDispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( CatchupClientProtocol.State.STORE_ID, new GetStoreIdResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.TX_PULL_RESPONSE, new TxPullResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.CORE_SNAPSHOT, new CoreSnapshotDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.STORE_COPY_FINISHED, new StoreCopyFinishedResponseDecoderV3() );
        decoderDispatcher.register( CatchupClientProtocol.State.TX_STREAM_FINISHED, new TxStreamFinishedResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.FILE_HEADER, new FileHeaderDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.PREPARE_STORE_COPY_RESPONSE, new PrepareStoreCopyResponse.Decoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.FILE_CONTENTS, new FileChunkDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.ERROR_RESPONSE, new CatchupErrorResponseDecoder() );

        pipelineBuilder.client( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "enc_req_tx", new TxPullRequestEncoderV2() )
                .add( "enc_req_index", new GetIndexFilesRequestMarshalV2.Encoder() )
                .add( "enc_req_store", new GetStoreFileRequestMarshalV2.Encoder() )
                .add( "enc_req_snapshot", new CoreSnapshotRequestEncoder() )
                .add( "enc_req_store_id", new GetStoreIdRequestEncoderV2() )
                .add( "enc_req_type", new ResponseMessageTypeEncoder() )
                .add( "enc_res_type", new RequestMessageTypeEncoder() )
                .add( "enc_req_precopy", new PrepareStoreCopyRequestEncoderV2() )
                .add( "in_res_type", new ClientMessageTypeHandler( protocol, logProvider ) )
                .add( "dec_dispatch", decoderDispatcher )
                .add( "hnd_res_tx", new TxPullResponseHandler( protocol, handler ) )
                .add( "hnd_res_snapshot", new CoreSnapshotResponseHandler( protocol, handler ) )
                .add( "hnd_res_copy_fin", new StoreCopyFinishedResponseHandler( protocol, handler ) )
                .add( "hnd_res_tx_fin", new TxStreamFinishedResponseHandler( protocol, handler ) )
                .add( "hnd_res_file_header", new FileHeaderHandler( protocol, handler, logProvider ) )
                .add( "hnd_res_file_chunk", new FileChunkHandler( protocol, handler ) )
                .add( "hnd_res_store_id", new GetStoreIdResponseHandler( protocol, handler ) )
                .add( "hnd_res_store_listing", new StoreListingResponseHandler( protocol, handler ))
                .add( "hnd_res_catchup_error", new CatchupErrorResponseHandler( protocol, handler ) )
                .install();
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

