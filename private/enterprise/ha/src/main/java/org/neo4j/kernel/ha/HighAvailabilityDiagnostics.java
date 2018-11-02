/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.internal.diagnostics.DiagnosticsPhase;
import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.logging.Logger;

/**
 * TODO
 */
public class HighAvailabilityDiagnostics
        implements DiagnosticsProvider
{
    private final HighAvailabilityMemberStateMachine memberStateMachine;
    private final ClusterClient clusterClient;

    public HighAvailabilityDiagnostics( HighAvailabilityMemberStateMachine memberStateMachine,
                                        ClusterClient clusterClient )
    {
        this.memberStateMachine = memberStateMachine;
        this.clusterClient = clusterClient;
    }

    @Override
    public String getDiagnosticsIdentifier()
    {
        return getClass().getSimpleName();
    }

    @Override
    public void acceptDiagnosticsVisitor( Object visitor )
    {
    }

    @Override
    public void dump( DiagnosticsPhase phase, Logger logger )
    {
        StringBuilder builder = new StringBuilder();

        builder.append( "High Availability diagnostics\n" ).
                append( "Member state:" ).append( memberStateMachine.getCurrentState().name() ).append( "\n" ).
                append( "State machines:\n" );

        clusterClient.dumpDiagnostics( builder );
        logger.log( builder.toString() );
    }
}
