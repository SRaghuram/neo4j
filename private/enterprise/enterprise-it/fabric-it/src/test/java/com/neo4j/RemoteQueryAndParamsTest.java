/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.fabric.driver.AutoCommitStatementResult;
import com.neo4j.fabric.driver.DriverPool;
import com.neo4j.fabric.driver.FabricDriverTransaction;
import com.neo4j.fabric.driver.PooledDriver;
import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import scala.Option;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.configuration.helpers.NormalizedGraphName;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Transaction;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.string.UTF8;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static scala.collection.JavaConverters.asScalaBuffer;

class RemoteQueryAndParamsTest
{

    private static TestFabric testFabric;
    private static Driver clientDriver;
    private static DriverPool driverPool = mock( DriverPool.class );
    private static final DriverUtils driverUtils = new DriverUtils( "mega" );

    private FabricDriverTransaction remoteTx;

    @BeforeAll
    static void beforeAll()
    {
        var additionalSettings = Map.of(
                "fabric.graph.0.uri", "bolt://localhost:1111",
                "fabric.graph.1.uri", "bolt://localhost:2222"
        );

        testFabric = new TestFabricFactory()
                .withFabricDatabase( "mega" )
                .withAdditionalSettings( additionalSettings )
                .addMocks( driverPool, new TestCatalogManager() )
                .build();

        clientDriver = testFabric.directClientDriver();
    }

    @BeforeEach
    void beforeEach()
    {
        remoteTx = createMockFabricDriverTransaction( mockResult() );
        PooledDriver driver = mock( PooledDriver.class );
        when( driver.beginTransaction( any(), any(), any(), any() ) ).thenReturn( Mono.just( remoteTx ) );
        doReturn( driver ).when( driverPool ).getDriver( any(), any() );
    }

    @AfterAll
    static void afterAll()
    {
        testFabric.close();
    }

    private static AutoCommitStatementResult mockResult()
    {
        AutoCommitStatementResult statementResult = mock( AutoCommitStatementResult.class );
        when( statementResult.columns() ).thenReturn( Flux.empty() );
        when( statementResult.records() ).thenReturn( Flux.empty() );
        when( statementResult.summary() ).thenReturn( Mono.empty() );

        return statementResult;
    }

    private static FabricDriverTransaction createMockFabricDriverTransaction( StatementResult mockStatementResult )
    {
        FabricDriverTransaction tx = mock( FabricDriverTransaction.class );

        when( tx.run( any(), any() ) ).thenReturn( mockStatementResult );
        when( tx.commit() ).thenReturn( Mono.empty() );
        when( tx.rollback() ).thenReturn( Mono.empty() );

        return tx;
    }

    static class TestCatalogManager implements CatalogManager
    {

        @SuppressWarnings( "unchecked" )
        @Override
        public Catalog currentCatalog()
        {
            return Catalog.create(
                    asScalaBuffer( List.of(
                            new Catalog.InternalGraph( 2L, new UUID( 2, 0 ), new NormalizedGraphName( "system" ), new NormalizedDatabaseName( "system" ) )
                    ) ),
                    asScalaBuffer( List.of(
                            new Catalog.ExternalGraph( 0L, Option.empty(), new UUID( 0, 0 ) )
                    ) ),
                    Option.apply( "mega" )
            );
        }

        @Override
        public Location locationOf( Catalog.Graph graph, boolean requireWritable, boolean canRoute )
        {
            if ( graph instanceof Catalog.InternalGraph )
            {
                Catalog.InternalGraph internalGraph = (Catalog.InternalGraph) graph;
                return new Location.Remote.Internal( internalGraph.id(),
                                                     internalGraph.uuid(),
                                                     new Location.RemoteUri( "neo4j", List.of(), "" ),
                                                     internalGraph.databaseName().name() );
            }
            else
            {
                Catalog.ExternalGraph externalGraph = (Catalog.ExternalGraph) graph;
                return new Location.Remote.External( externalGraph.id(),
                                                     externalGraph.uuid(),
                                                     new Location.RemoteUri( "neo4j", List.of(), "" ),
                                                     "neo4j" );
            }
        }
    }

    @Test
    void testRemoteQuery()
    {
        String query = joinAsLines(
                "USE mega.graph(0)",
                "RETURN 1 AS x"
        );

        doInMegaTx( AccessMode.READ, tx -> tx.run( query, Map.of( "a", "x", "b", 2L ) ).consume() );

        verify( remoteTx, times( 1 ) )
                .run( eq( "RETURN 1 AS `x`" ),
                      eq( mapValue( Map.of( "a", "x", "b", 2L ) ) ) );
    }

    @Test
    void testRemoteAdminCommand()
    {
        String query = joinAsLines(
                "CREATE USER myUser SET PASSWORD 'secret'"
        );

        doInMegaTx( AccessMode.WRITE, tx -> tx.run( query, Map.of( "a", "x", "b", 2L ) ).consume() );

        verify( remoteTx, times( 1 ) )
                .run( eq( "CREATE USER myUser SET PASSWORD $`  AUTOSTRING0` CHANGE REQUIRED" ),
                      eq( mapValue( Map.of( "a", "x", "b", 2L, "  AUTOSTRING0", UTF8.encode( "secret" ) ) ) ) );
    }

    @Test
    void testRemoteAdminProcedure()
    {
        String query = joinAsLines(
                "USE system",
                "CALL dbms.security.createUser('myUser', 'secret')"
        );

        doInMegaTx( AccessMode.WRITE, tx -> tx.run( query, Map.of( "a", "x", "b", 2L ) ).consume() );

        verify( remoteTx, times( 1 ) )
                .run( eq( "CALL `dbms`.`security`.`createUser`(\"myUser\", \"secret\")" ),
                      eq( mapValue( Map.of( "a", "x", "b", 2L ) ) ) );
    }

    private MapValue mapValue( Map<String,Object> map )
    {
        final String[] keys = map.keySet().toArray( new String[]{} );
        final AnyValue[] vals = map.values().stream().map( Values::of ).collect( Collectors.toList() ).toArray( new AnyValue[]{} );

        return VirtualValues.map( keys, vals );
    }

    private void doInMegaTx( AccessMode mode, Consumer<Transaction> workload )
    {
        driverUtils.doInTx( clientDriver, mode, workload );
    }
}
