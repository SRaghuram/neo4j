/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPlugin;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;
import static org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;
import static org.neo4j.values.storable.Values.stringValue;

class GetRoutingTableProcedureForMultiDCTest
{
    @Test
    void shouldHaveCorrectSignature()
    {
        // given
        var proc = newProcedure( null );

        // when
        var signature = proc.signature();

        // then
        assertEquals( List.of( inputField( "context", NTMap ), inputField( "database", NTString, nullValue( NTString ) ) ), signature.inputSignature() );
        assertEquals( List.of( outputField( "ttl", NTInteger ), outputField( "servers", NTList( NTMap ) ) ), signature.outputSignature() );
        assertTrue( signature.systemProcedure() );
    }

    @Test
    void shouldPassClientContextToPlugin() throws Exception
    {
        // given
        var databaseManager = new StubClusteredDatabaseManager();
        var databaseId = databaseManager.databaseIdRepository().getByName( "my-database" ).get();
        var availabilityGuard = mock( DatabaseAvailabilityGuard.class );
        when( availabilityGuard.isAvailable() ).thenReturn( true );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId ).withDatabaseAvailabilityGuard( availabilityGuard ).register();

        var plugin = mock( LoadBalancingPlugin.class );
        var addresses = List.of( new SocketAddress( "localhost", 12345 ) );
        var result = new RoutingResult( addresses, addresses, addresses, 100 );
        when( plugin.run( any(), any( MapValue.class ) ) ).thenReturn( result );
        var proc = newProcedure( plugin, databaseManager );
        var clientContext = ValueUtils.asMapValue( map( "key", "value", "key2", "value2" ) );

        // when
        proc.apply( null, new AnyValue[]{clientContext, stringValue( databaseId.name() )}, null );

        // then
        verify( plugin ).run( databaseId, clientContext );
    }

    @Test
    void shouldHaveCorrectNamespace()
    {
        // given
        var plugin = mock( LoadBalancingPlugin.class );

        CallableProcedure proc = newProcedure( plugin );

        // when
        var name = proc.signature().name();

        // then
        assertEquals( new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ), name );
    }

    @Test
    void shouldThrowWhenDatabaseDoesNotExist()
    {
        var databaseName = "cars";
        TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
        var databaseManager = new StubClusteredDatabaseManager( databaseIdRepository );
        databaseIdRepository.filter( databaseName );

        var plugin = mock( LoadBalancingPlugin.class );
        var proc = newProcedure( plugin, databaseManager );

        var error = assertThrows( ProcedureException.class, () -> proc.apply( null, new AnyValue[]{MapValue.EMPTY, stringValue( databaseName )}, null ) );
        assertEquals( Status.Database.DatabaseNotFound, error.status() );
    }

    @Test
    void shouldNotThrowWhenDatabaseIsStopped() throws ProcedureException
    {
        var databaseManager = new StubClusteredDatabaseManager();
        var databaseId = databaseManager.databaseIdRepository().getByName( "my-database" ).get();
        var availabilityGuard = mock( DatabaseAvailabilityGuard.class );
        when( availabilityGuard.isAvailable() ).thenReturn( false );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId ).withDatabaseAvailabilityGuard( availabilityGuard ).register();

        var plugin = mock( LoadBalancingPlugin.class );
        var addresses = List.of( new SocketAddress( "localhost", 12345 ) );
        var result = new RoutingResult( addresses, addresses, addresses, 100 );
        when( plugin.run( any(), any( MapValue.class ) ) ).thenReturn( result );
        var proc = newProcedure( plugin, databaseManager );

        proc.apply( null, new AnyValue[]{MapValue.EMPTY, stringValue( databaseId.name() )}, null );
        // then
        verify( plugin ).run( databaseId, MapValue.EMPTY );
    }

    @Test
    void shouldThrowWhenPluginReturnsAnEmptyRoutingTable() throws Exception
    {
        var databaseManager = new StubClusteredDatabaseManager();
        var databaseId = databaseManager.databaseIdRepository().getByName( "customers" ).get();
        var availabilityGuard = mock( DatabaseAvailabilityGuard.class );
        when( availabilityGuard.isAvailable() ).thenReturn( true );
        databaseManager.givenDatabaseWithConfig().withDatabaseId( databaseId ).withDatabaseAvailabilityGuard( availabilityGuard ).register();

        var plugin = mock( LoadBalancingPlugin.class );
        when( plugin.run( any(), any() ) ).thenReturn( new RoutingResult( emptyList(), emptyList(), emptyList(), 42 ) );
        var proc = newProcedure( plugin, databaseManager );

        var error = assertThrows( ProcedureException.class, () -> proc.apply( null, new AnyValue[]{MapValue.EMPTY, stringValue( databaseId.name() )}, null ) );
        assertEquals( Status.Database.DatabaseUnavailable, error.status() );
        verify( plugin ).run( databaseId, MapValue.EMPTY );
    }

    private static GetRoutingTableProcedureForMultiDC newProcedure( LoadBalancingPlugin loadBalancingPlugin )
    {
        return newProcedure( loadBalancingPlugin, new StubClusteredDatabaseManager() );
    }

    private static GetRoutingTableProcedureForMultiDC newProcedure( LoadBalancingPlugin loadBalancingPlugin, DatabaseManager<?> databaseManager )
    {
        return new GetRoutingTableProcedureForMultiDC( DEFAULT_NAMESPACE, loadBalancingPlugin, databaseManager, Config.defaults(), nullLogProvider() );
    }
}
