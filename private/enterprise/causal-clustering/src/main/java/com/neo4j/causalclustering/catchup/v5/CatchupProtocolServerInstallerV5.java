/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v5;

import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstallerV3;
import com.neo4j.causalclustering.catchup.v4.CatchupProtocolServerInstallerV4;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;

import java.util.List;

import org.neo4j.logging.LogProvider;

public class CatchupProtocolServerInstallerV5 extends CatchupProtocolServerInstallerV4
{
    private static final ApplicationProtocols APPLICATION_PROTOCOL = ApplicationProtocols.CATCHUP_5_0;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,CatchupProtocolServerInstallerV3>
    {
        public Factory( NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider, CatchupServerHandler catchupServerHandler )
        {
            super( APPLICATION_PROTOCOL,
                   modifiers -> new CatchupProtocolServerInstallerV5( pipelineBuilderFactory, modifiers, logProvider, catchupServerHandler ) );
        }
    }

    public CatchupProtocolServerInstallerV5( NettyPipelineBuilderFactory pipelineBuilderFactory,
                                             List<ModifierProtocolInstaller<Orientation.Server>> modifiers,
                                             LogProvider logProvider, CatchupServerHandler catchupServerHandler )
    {
        super( pipelineBuilderFactory, modifiers, logProvider, catchupServerHandler );
    }
}
