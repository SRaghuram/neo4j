/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;
import static org.neo4j.values.storable.Values.stringValue;

class GetRoutingTableProcedureForMultiDCTest
{
    @Test
    void shouldHaveCorrectSignature()
    {
        // given
        GetRoutingTableProcedureForMultiDC proc = newProcedure( null );

        // when
        ProcedureSignature signature = proc.signature();

        // then
        assertEquals( List.of( inputField( "context", NTMap ), inputField( "database", NTString, nullValue( NTString ) ) ), signature.inputSignature() );

        assertEquals( List.of( outputField( "ttl", NTInteger ), outputField( "servers", NTList( NTMap ) ) ), signature.outputSignature() );
    }

    @Test
    void shouldPassClientContextToPlugin() throws Exception
    {
        // given
        LoadBalancingPlugin plugin = mock( LoadBalancingPlugin.class );
        List<AdvertisedSocketAddress> addresses = List.of( new AdvertisedSocketAddress( "localhost", 12345 ) );
        RoutingResult result = new RoutingResult( addresses, addresses, addresses, 100 );
        when( plugin.run( any( DatabaseId.class ), any( MapValue.class ) ) ).thenReturn( result );
        GetRoutingTableProcedureForMultiDC proc = newProcedure( plugin );
        MapValue clientContext = ValueUtils.asMapValue( map( "key", "value", "key2", "value2" ) );

        // when
        proc.apply( null, new AnyValue[]{clientContext, stringValue( "my_database" )}, null );

        // then
        verify( plugin ).run( new DatabaseId( "my_database" ), clientContext );
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

    private static GetRoutingTableProcedureForMultiDC newProcedure( LoadBalancingPlugin loadBalancingPlugin )
    {
        return new GetRoutingTableProcedureForMultiDC( DEFAULT_NAMESPACE, loadBalancingPlugin, Config.defaults() );
    }
}
