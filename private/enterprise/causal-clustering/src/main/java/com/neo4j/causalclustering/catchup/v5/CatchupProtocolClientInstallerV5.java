/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v5;

import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolClientInstallerV3;
import com.neo4j.causalclustering.catchup.v4.CatchupProtocolClientInstallerV4;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;

import java.util.List;

import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class CatchupProtocolClientInstallerV5 extends CatchupProtocolClientInstallerV4
{
    private static final ApplicationProtocols APPLICATION_PROTOCOL = ApplicationProtocols.CATCHUP_5_0;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client,CatchupProtocolClientInstallerV3>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilder, LogProvider logProvider, CatchupResponseHandler handler,
                        CommandReaderFactory commandReaderFactory )
        {
            super( APPLICATION_PROTOCOL,
                   modifiers -> new CatchupProtocolClientInstallerV5( pipelineBuilder, modifiers, logProvider, handler, commandReaderFactory ) );
        }
    }

    public CatchupProtocolClientInstallerV5( NettyPipelineBuilderFactory pipelineBuilder,
                                             List<ModifierProtocolInstaller<Orientation.Client>> modifiers,
                                             LogProvider logProvider, CatchupResponseHandler handler,
                                             CommandReaderFactory commandReaderFactory )
    {
        super( pipelineBuilder, modifiers, logProvider, handler, commandReaderFactory );
    }
}
