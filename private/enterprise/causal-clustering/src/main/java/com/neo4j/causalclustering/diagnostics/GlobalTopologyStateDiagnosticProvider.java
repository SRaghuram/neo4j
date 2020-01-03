/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.diagnostics;

import com.neo4j.causalclustering.discovery.TopologyService;

import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.logging.Logger;

import static java.lang.System.lineSeparator;
import static org.neo4j.internal.helpers.Strings.printMap;

public class GlobalTopologyStateDiagnosticProvider implements DiagnosticsProvider
{

    private final TopologyService topologyService;

    public GlobalTopologyStateDiagnosticProvider( TopologyService topologyService )
    {
        this.topologyService = topologyService;
    }

    @Override
    public String getDiagnosticsName()
    {
        return "Global topology state";
    }

    @Override
    public void dump( Logger logger )
    {
        logger.log( "Current core topology:%s%s", newPaddedLIne(), printMap( topologyService.allCoreServers(), newPaddedLIne() ) );
        logger.log( "Current read replica topology:%s%s", newPaddedLIne(), printMap( topologyService.allReadReplicas(), newPaddedLIne() ) );
    }

    private static String newPaddedLIne()
    {
        return lineSeparator() + "  ";
    }
}
