/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetServersProcedureForMultiDC;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetServersProcedureForSingleDC;
import org.junit.jupiter.api.Test;

import java.util.Set;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.proc.GlobalProcedures;
import org.neo4j.logging.NullLogProvider;

import static java.util.stream.Collectors.toSet;
import static org.eclipse.collections.impl.set.mutable.UnifiedSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CoreRoutingProcedureInstallerTest
{
    @Test
    void shouldRegisterSingleDCProcedures() throws Exception
    {
        Config config = newConfig( false );
        GlobalProcedures procedures = spy( new GlobalProcedures() );

        installRoutingProcedures( config, procedures );

        verifyRegisteredProcedureNames( procedures );
        verify( procedures, times( 2 ) ).register( any( GetServersProcedureForSingleDC.class ) );
    }

    @Test
    void shouldRegisterMultiDCProcedures() throws Exception
    {
        Config config = newConfig( true );
        GlobalProcedures procedures = spy( new GlobalProcedures() );

        installRoutingProcedures( config, procedures );

        verifyRegisteredProcedureNames( procedures );
        verify( procedures, times( 2 ) ).register( any( GetServersProcedureForMultiDC.class ) );
    }

    private static Config newConfig( boolean multiDC )
    {
        Config config = Config.defaults();
        config.augment( CausalClusteringSettings.multi_dc_license, Boolean.toString( multiDC ) );
        return config;
    }

    private static void installRoutingProcedures( Config config, GlobalProcedures procedures ) throws ProcedureException
    {
        TopologyService topologyService = mock( TopologyService.class );
        LeaderLocator leaderLocator = mock( LeaderLocator.class );

        CoreRoutingProcedureInstaller installer = new CoreRoutingProcedureInstaller( topologyService, leaderLocator, config, NullLogProvider.getInstance() );
        installer.install( procedures );
    }

    private static void verifyRegisteredProcedureNames( GlobalProcedures procedures )
    {
        Set<QualifiedName> expectedNames = newSetWith(
                new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ),
                new QualifiedName( new String[]{"dbms", "cluster", "routing"}, "getRoutingTable" ) );

        Set<QualifiedName> actualNames = procedures.getAllProcedures()
                .stream()
                .map( ProcedureSignature::name )
                .collect( toSet() );

        assertEquals( expectedNames, actualNames );
    }
}
