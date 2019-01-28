/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import org.junit.Test;

import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;

public class GetServersProcedureV2Test
{
    @Test
    public void shouldHaveCorrectSignature()
    {
        // given
        GetServersProcedureForMultiDC proc = new GetServersProcedureForMultiDC( null );

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
    public void shouldPassClientContextToPlugin() throws Exception
    {
        // given
        LoadBalancingPlugin plugin = mock( LoadBalancingPlugin.class );
        LoadBalancingProcessor.Result result = mock( LoadBalancingPlugin.Result.class );
        when( plugin.run( any( MapValue.class ) ) ).thenReturn( result );
        GetServersProcedureForMultiDC getServers = new GetServersProcedureForMultiDC( plugin );
        MapValue clientContext = ValueUtils.asMapValue( map( "key", "value", "key2", "value2" ) );

        // when
        getServers.apply( null, new AnyValue[]{clientContext}, null );

        // then
        verify( plugin ).run( clientContext );
    }
}
