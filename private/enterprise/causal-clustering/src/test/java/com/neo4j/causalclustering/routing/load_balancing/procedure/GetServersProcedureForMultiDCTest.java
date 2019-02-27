/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;

class GetServersProcedureForMultiDCTest
{
    @Test
    void shouldHaveCorrectSignature()
    {
        // given
        GetServersProcedureForMultiDC proc = newProcedure( null );

        // when
        ProcedureSignature signature = proc.signature();

        // then
        assertThat( signature.inputSignature(), containsInAnyOrder(
                FieldSignature.inputField( "context", NTMap ) ) );

        assertThat( signature.outputSignature(), containsInAnyOrder(
                FieldSignature.outputField( "ttl", NTInteger ),
                FieldSignature.outputField( "servers", NTList( NTMap ) ) ) );
    }

    @Test
    void shouldPassClientContextToPlugin() throws Exception
    {
        // given
        LoadBalancingPlugin plugin = mock( LoadBalancingPlugin.class );
        RoutingResult result = new RoutingResult( emptyList(), emptyList(), emptyList(), 100 );
        when( plugin.run( any( MapValue.class ) ) ).thenReturn( result );
        GetServersProcedureForMultiDC getServers = newProcedure( plugin );
        MapValue clientContext = ValueUtils.asMapValue( map( "key", "value", "key2", "value2" ) );

        // when
        getServers.apply( null, new AnyValue[]{clientContext}, null );

        // then
        verify( plugin ).run( clientContext );
    }

    @Test
    void shouldHaveCorrectNamespace()
    {
        // given
        LoadBalancingPlugin plugin = mock( LoadBalancingPlugin.class );

        CallableProcedure proc = newProcedure( plugin );

        // when
        QualifiedName name = proc.signature().name();

        // then
        assertEquals( new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ), name );
    }

    private static GetServersProcedureForMultiDC newProcedure( LoadBalancingPlugin loadBalancingPlugin )
    {
        return new GetServersProcedureForMultiDC( DEFAULT_NAMESPACE, loadBalancingPlugin );
    }
}
