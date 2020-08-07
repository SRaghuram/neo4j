/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3;

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
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseDecoder;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponseHandler;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdRequestEncoder;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponseDecoder;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponseHandler;
import com.neo4j.causalclustering.catchup.v3.storecopy.CatchupErrorResponseDecoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.CatchupErrorResponseHandler;
import com.neo4j.causalclustering.catchup.v3.storecopy.CoreSnapshotRequestEncoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequestEncoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequestEncoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequestEncoder;
import com.neo4j.causalclustering.catchup.v3.storecopy.StoreCopyFinishedResponseDecoder;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequestEncoder;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullResponseDecoder;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullResponseHandler;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotDecoder;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotResponseHandler;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class CatchupProtocolClientInstaller implements ProtocolInstaller<ProtocolInstaller.Orientation.Client>
{
    private static final ApplicationProtocols APPLICATION_PROTOCOL = ApplicationProtocols.CATCHUP_3_0;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client,CatchupProtocolClientInstaller>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilder, LogProvider logProvider, CatchupResponseHandler handler,
                CommandReaderFactory commandReaderFactory )
        {
            super( APPLICATION_PROTOCOL,
                    modifiers -> new CatchupProtocolClientInstaller( pipelineBuilder, modifiers, logProvider, handler, commandReaderFactory ) );
        }
    }

    private final List<ModifierProtocolInstaller<Orientation.Client>> modifiers;
    private final LogProvider logProvider;
    private final Log log;
    private final NettyPipelineBuilderFactory pipelineBuilder;
    private final CatchupResponseHandler handler;
    private final CommandReaderFactory commandReaderFactory;

    public CatchupProtocolClientInstaller( NettyPipelineBuilderFactory pipelineBuilder, List<ModifierProtocolInstaller<Orientation.Client>> modifiers,
            LogProvider logProvider, CatchupResponseHandler handler, CommandReaderFactory commandReaderFactory )
    {
        this.modifiers = modifiers;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.pipelineBuilder = pipelineBuilder;
        this.handler = handler;
        this.commandReaderFactory = commandReaderFactory;
    }

    /**
     * Uses latest version of handlers. Hence version naming may be less than the current version if no change was needed for that handler
     */
    @Override
    public void install( Channel channel )
    {
        CatchupClientProtocol protocol = new CatchupClientProtocol();

        RequestDecoderDispatcher<CatchupClientProtocol.State> decoderDispatcher = new RequestDecoderDispatcher<>( protocol, logProvider );
        decoderDispatcher.register( CatchupClientProtocol.State.STORE_ID, new GetStoreIdResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.DATABASE_ID, new GetDatabaseIdResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.TX_PULL_RESPONSE, new TxPullResponseDecoder( commandReaderFactory ) );
        decoderDispatcher.register( CatchupClientProtocol.State.CORE_SNAPSHOT, new CoreSnapshotDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.STORE_COPY_FINISHED, new StoreCopyFinishedResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.TX_STREAM_FINISHED, new TxStreamFinishedResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.FILE_HEADER, new FileHeaderDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.PREPARE_STORE_COPY_RESPONSE, new PrepareStoreCopyResponse.Decoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.FILE_CHUNK, new FileChunkDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.ERROR_RESPONSE, new CatchupErrorResponseDecoder() );

        pipelineBuilder.client( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "enc_req_tx", new TxPullRequestEncoder() )
                .add( "enc_req_store", new GetStoreFileRequestEncoder() )
                .add( "enc_req_snapshot", new CoreSnapshotRequestEncoder() )
                .add( "enc_req_store_id", new GetStoreIdRequestEncoder() )
                .add( "enc_req_database_id", new GetDatabaseIdRequestEncoder() )
                .add( "enc_req_type", new ResponseMessageTypeEncoder() )
                .add( "enc_res_type", new RequestMessageTypeEncoder() )
                .add( "enc_req_precopy", new PrepareStoreCopyRequestEncoder() )
                .add( "in_res_type", new ClientMessageTypeHandler( protocol, logProvider ) )
                .add( "dec_dispatch", decoderDispatcher )
                .add( "hnd_res_tx", new TxPullResponseHandler( protocol, handler ) )
                .add( "hnd_res_snapshot", new CoreSnapshotResponseHandler( protocol, handler ) )
                .add( "hnd_res_copy_fin", new StoreCopyFinishedResponseHandler( protocol, handler ) )
                .add( "hnd_res_tx_fin", new TxStreamFinishedResponseHandler( protocol, handler ) )
                .add( "hnd_res_file_header", new FileHeaderHandler( protocol, handler ) )
                .add( "hnd_res_file_chunk", new FileChunkHandler( protocol, handler ) )
                .add( "hnd_res_store_id", new GetStoreIdResponseHandler( protocol, handler ) )
                .add( "hnd_res_database_id", new GetDatabaseIdResponseHandler( protocol, handler ) )
                .add( "hnd_res_store_listing", new StoreListingResponseHandler( protocol, handler ))
                .add( "hnd_res_catchup_error", new CatchupErrorResponseHandler( protocol, handler ) )
                .install();
    }

    @Override
    public Collection<Collection<ModifierProtocol>> modifiers()
    {
        return modifiers.stream()
                .map( ModifierProtocolInstaller::protocols )
                .collect( Collectors.toList() );
    }
}

