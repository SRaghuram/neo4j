/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4;

import com.neo4j.causalclustering.catchup.CatchupClientProtocol;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import com.neo4j.causalclustering.catchup.RequestDecoderDispatcher;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolClientInstallerV3;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsRequestEncoder;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponseDecoder;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponseHandler;
import com.neo4j.causalclustering.catchup.v4.info.InfoRequestEncoder;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponseDecoder;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponseHandler;
import com.neo4j.causalclustering.protocol.ClientNettyPipelineBuilder;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;

import java.util.List;

import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class CatchupProtocolClientInstallerV4 extends CatchupProtocolClientInstallerV3
{
    private static final ApplicationProtocols APPLICATION_PROTOCOL = ApplicationProtocols.CATCHUP_4_0;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client,CatchupProtocolClientInstallerV3>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilder, LogProvider logProvider, CatchupResponseHandler handler,
                        CommandReaderFactory commandReaderFactory )
        {
            super( APPLICATION_PROTOCOL,
                   modifiers -> new CatchupProtocolClientInstallerV4( pipelineBuilder, modifiers, logProvider, handler, commandReaderFactory ) );
        }
    }

    public CatchupProtocolClientInstallerV4( NettyPipelineBuilderFactory pipelineBuilder,
            List<ModifierProtocolInstaller<Orientation.Client>> modifiers,
            LogProvider logProvider, CatchupResponseHandler handler,
            CommandReaderFactory commandReaderFactory )
    {
        super( pipelineBuilder, modifiers, logProvider, handler, commandReaderFactory );
    }

    @Override
    protected ClientNettyPipelineBuilder encoders( ClientNettyPipelineBuilder builder )
    {
        return super.encoders( builder )
                .add( "enc_get_all_database_ids", new GetAllDatabaseIdsRequestEncoder() )
                .add( "enc_req_info", new InfoRequestEncoder() );
    }

    @Override
    protected ClientNettyPipelineBuilder handlers( ClientNettyPipelineBuilder builder, CatchupClientProtocol protocol )
    {
        return super.handlers( builder, protocol )
                .add( "hnd_info", new InfoResponseHandler( handler(), protocol ) )
                .add( "hnd_get_all_database_ids", new GetAllDatabaseIdsResponseHandler( handler(), protocol ) );
    }

    @Override
    protected void decoders( RequestDecoderDispatcher<CatchupClientProtocol.State> decoderDispatcher )
    {
        super.decoders( decoderDispatcher );
        decoderDispatcher.register( CatchupClientProtocol.State.GET_ALL_DATABASE_IDS, new GetAllDatabaseIdsResponseDecoder() );
        decoderDispatcher.register( CatchupClientProtocol.State.GET_INFO, new InfoResponseDecoder() );
    }
}
