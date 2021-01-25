/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.filters.IdentityFilter;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerInfo;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetRoutingTableProcedureForMultiDC;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetRoutingTableProcedureForSingleDC;
import com.neo4j.configuration.CausalClusteringSettings;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static com.neo4j.causalclustering.core.CoreRoutingProcedureInstaller.MULTI_DC_NOT_ENABLED_WARN;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.LogAssertions.assertThat;

class CoreRoutingProcedureInstallerTest
{
    @Test
    void shouldRegisterSingleDCProcedures() throws Exception
    {
        var config = newConfig( false );
        var procedures = spy( new GlobalProceduresRegistry() );

        installRoutingProcedures( config, procedures, NullLogProvider.getInstance() );

        verifyRegisteredProcedureNames( procedures );
        verify( procedures, times( 2 ) ).register( any( GetRoutingTableProcedureForSingleDC.class ) );
    }

    @Test
    void shouldRegisterMultiDCProcedures() throws Exception
    {
        var config = newConfig( true );
        var procedures = spy( new GlobalProceduresRegistry() );

        installRoutingProcedures( config, procedures, NullLogProvider.getInstance() );

        verifyRegisteredProcedureNames( procedures );
        verify( procedures, times( 2 ) ).register( any( GetRoutingTableProcedureForMultiDC.class ) );
    }

    private static Config newConfig( boolean multiDC )
    {
        return Config.defaults( CausalClusteringSettings.multi_dc_license, multiDC );
    }

    private static void installRoutingProcedures( Config config, GlobalProcedures procedures, LogProvider logProvider ) throws ProcedureException
    {
        var topologyService = mock( TopologyService.class );
        var leaderService = mock( LeaderService.class );

        var installer = new CoreRoutingProcedureInstaller( topologyService, leaderService, new StubClusteredDatabaseManager(), config,
                                                           logProvider );
        installer.install( procedures );
    }

    private static void verifyRegisteredProcedureNames( GlobalProcedures procedures )
    {
        var expectedNames = Set.of(
                new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ),
                new QualifiedName( new String[]{"dbms", "cluster", "routing"}, "getRoutingTable" ) );

        var actualNames = procedures.getAllProcedures()
                                    .stream()
                                    .map( ProcedureSignature::name )
                                    .collect( toSet() );

        assertEquals( expectedNames, actualNames );
    }

    @Test
    void shouldWarnIfCustomLoadBalancerRequestedForSingleDC() throws ProcedureException
    {
        var config = Config.defaults(
                Map.of( CausalClusteringSettings.multi_dc_license, false,
                        CausalClusteringSettings.load_balancing_plugin, "some plugin"
                )
        );

        var procedures = new GlobalProceduresRegistry();

        var logger = new AssertableLogProvider();
        installRoutingProcedures( config, procedures, logger );
        assertThat( logger )
                .forLevel( AssertableLogProvider.Level.WARN )
                .containsMessages( MULTI_DC_NOT_ENABLED_WARN );
    }

    @Test
    void shouldWarnIfCustomServerPolicyProvidedButSingleDC() throws ProcedureException
    {
        var config = Config.defaults(
                Map.of( CausalClusteringSettings.multi_dc_license, false,
                        LoadBalancingServerPoliciesGroup.group( "default" ).value, IdentityFilter.<ServerInfo>as()
                )
        );

        var procedures = new GlobalProceduresRegistry();

        var logger = new AssertableLogProvider();
        installRoutingProcedures( config, procedures, logger );
        assertThat( logger )
                .forLevel( AssertableLogProvider.Level.WARN )
                .containsMessages( MULTI_DC_NOT_ENABLED_WARN );
    }
}
