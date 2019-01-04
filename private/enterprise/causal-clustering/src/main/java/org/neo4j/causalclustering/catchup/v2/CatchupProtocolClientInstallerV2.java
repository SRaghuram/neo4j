/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.v2;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.catchup.CatchupClientProtocol;
import org.neo4j.causalclustering.catchup.CatchupResponseHandler;
import org.neo4j.causalclustering.catchup.ClientMessageTypeHandler;
import org.neo4j.causalclustering.catchup.RequestDecoderDispatcher;
import org.neo4j.causalclustering.catchup.RequestMessageTypeEncoder;
import org.neo4j.causalclustering.catchup.ResponseMessageTypeEncoder;
import org.neo4j.causalclustering.catchup.StoreListingResponseHandler;
import org.neo4j.causalclustering.catchup.storecopy.FileChunkDecoder;
import org.neo4j.causalclustering.catchup.storecopy.FileChunkHandler;
import org.neo4j.causalclustering.catchup.storecopy.FileHeaderDecoder;
import org.neo4j.causalclustering.catchup.storecopy.FileHeaderHandler;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseDecoder;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponseHandler;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseDecoder;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponseHandler;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseDecoder;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseHandler;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseDecoder;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseHandler;
import org.neo4j.causalclustering.catchup.v2.storecopy.CatchupErrorResponseDecoder;
import org.neo4j.causalclustering.catchup.v2.storecopy.CatchupErrorResponseHandler;
import org.neo4j.causalclustering.catchup.v2.storecopy.GetIndexFilesRequestMarshalV2;
import org.neo4j.causalclustering.catchup.v2.storecopy.GetStoreFileRequestMarshalV2;
import org.neo4j.causalclustering.catchup.v2.storecopy.GetStoreIdRequestEncoderV2;
import org.neo4j.causalclustering.catchup.v2.storecopy.PrepareStoreCopyRequestEncoderV2;
import org.neo4j.causalclustering.catchup.v2.tx.TxPullRequestEncoderV2;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotDecoder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestEncoder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotResponseHandler;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class CatchupProtocolClientInstallerV2 implements ProtocolInstaller<Orientation.Client>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.CATCHUP_2;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client,CatchupProtocolClientInstallerV2>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilder, LogProvider logProvider, CatchupResponseHandler handler )
        {
            super( APPLICATION_PROTOCOL, modifiers -> new CatchupProtocolClientInstallerV2( pipelineBuilder, modifiers, logProvider, handler ) );
        }
    }

    private final List<ModifierProtocolInstaller<Orientation.Client>> modifiers;
    private final LogProvider logProvider;
    private final Log log;
    private final NettyPipelineBuilderFactory pipelineBuilder;
    private final CatchupResponseHandler handler;

    public CatchupProtocolClientInstallerV2( NettyPipelineBuilderFactory pipelineBuilder, List<ModifierProtocolInstaller<Orientation.Client>> modifiers,
            LogProvider logProvider, CatchupResponseHandler handler )
    {
        this.modifiers = modifiers;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.pipelineBuilder = pipelineBuilder;
        this.handler = handler;
    }

    @Override
    public void install( Channel channel ) throws Exception
    {
        CatchupClientProtocol protocol = new CatchupClientProtocol();

        RequestDecoderDispatcher<CatchupClientProtocol.State> decoderDispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( CatchupClientProtocol.State.STORE_ID, new GetStoreIdResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.TX_PULL_RESPONSE, new TxPullResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.CORE_SNAPSHOT, new CoreSnapshotDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.STORE_COPY_FINISHED, new StoreCopyFinishedResponseDecoder() );
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
                .onClose( handler::onClose )
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

