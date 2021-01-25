/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4;

import com.neo4j.causalclustering.catchup.CatchupInboundEventListener;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.RequestDecoderDispatcher;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstallerV3;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsRequestDecoder;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponseEncoder;
import com.neo4j.causalclustering.catchup.v4.info.InfoRequestDecoder;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponseEncoder;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataRequestDecoder;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataResponseEncoder;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ServerNettyPipelineBuilder;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;

import java.util.List;

import org.neo4j.logging.LogProvider;

public class CatchupProtocolServerInstallerV4 extends CatchupProtocolServerInstallerV3
{
    private static final ApplicationProtocols APPLICATION_PROTOCOL = ApplicationProtocols.CATCHUP_4_0;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,CatchupProtocolServerInstallerV3>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilderFactory,
                        LogProvider logProvider,
                        CatchupServerHandler catchupServerHandler,
                        CatchupInboundEventListener listener )
        {
            super( APPLICATION_PROTOCOL, modifiers ->
                    new CatchupProtocolServerInstallerV4( pipelineBuilderFactory, modifiers, logProvider, catchupServerHandler, listener ) );
        }
    }

    public CatchupProtocolServerInstallerV4( NettyPipelineBuilderFactory pipelineBuilderFactory,
                                             List<ModifierProtocolInstaller<Orientation.Server>> modifiers,
                                             LogProvider logProvider,
                                             CatchupServerHandler catchupServerHandler,
                                             CatchupInboundEventListener listener )
    {
        super( pipelineBuilderFactory, modifiers, logProvider, catchupServerHandler, listener );
    }

    @Override
    protected ServerNettyPipelineBuilder encoders( ServerNettyPipelineBuilder builder, CatchupServerProtocol state )
    {
        return super.encoders( builder, state )
                    .add( "enc_res_all_databases_id", new GetAllDatabaseIdsResponseEncoder() )
                    .add( "enc_res_info", new InfoResponseEncoder() )
                    .add( "enc_res_metadata", new GetMetadataResponseEncoder() );
    }

    @Override
    protected ServerNettyPipelineBuilder handlers( ServerNettyPipelineBuilder builder, CatchupServerProtocol state )
    {
        return super.handlers( builder, state )
                    .add( "hnd_req_get_info", handler().getInfo( state ) )
                    .add( "hnd_req_all_databases_id", handler().getAllDatabaseIds( state ) )
                    .add( "hnd_req_metadata", handler().getMetadata( state ) );
    }

    @Override
    protected void decoders( RequestDecoderDispatcher<CatchupServerProtocol.State> decoderDispatcher )
    {
        super.decoders( decoderDispatcher );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_ALL_DATABASE_IDS, new GetAllDatabaseIdsRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_INFO, new InfoRequestDecoder() );
        decoderDispatcher.register( CatchupServerProtocol.State.GET_METADATA, new GetMetadataRequestDecoder() );
    }
}
