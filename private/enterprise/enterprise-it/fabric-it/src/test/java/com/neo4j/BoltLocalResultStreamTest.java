/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.ProxyFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Flux;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Mono;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import static org.assertj.core.api.Assertions.assertThat;

class BoltLocalResultStreamTest
{
    private static Driver clientDriver;
    private static TestServer testServer;

    @BeforeAll
    static void setUp() throws KernelException
    {
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "neo4j://somewhere:6666",
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true"
        );

        Config config = Config.newBuilder().setRaw( configProperties ).build();
        testServer = new TestServer( config );

        testServer.start();

        testServer.getDependencies().resolveDependency( GlobalProcedures.class )
                .registerFunction( ProxyFunctions.class );

        clientDriver = GraphDatabase.driver(
                testServer.getBoltRoutingUri(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withMaxConnectionPoolSize( 3 )
                        .withoutEncryption()
                        .build() );
    }

    @AfterAll
    static void tearDown()
    {
        testServer.stop();
        clientDriver.closeAsync();
    }

    @Test
    void testBasicResultStream()
    {
        List<String> result = inMegaTx( tx ->
                tx.run( "UNWIND range(0, 4) AS i RETURN 'r' + i as A" ).stream()
                        .map( r -> r.get( "A" ).asString() )
                        .collect( Collectors.toList() )
        );

        assertThat( result ).isEqualTo( List.of( "r0", "r1", "r2", "r3", "r4" ) );
    }

    @Test
    void testRxResultStream()
    {
        List<String> result = inMegaRxTx( tx ->
        {
            RxResult statementResult = tx.run( "UNWIND range(0, 4) AS i RETURN 'r' + i as A" );
            return Flux.from( statementResult.records() )
                    .limitRate( 1 )
                    .collectList()
                    .block()
                    .stream()
                    .map( r -> r.get( "A" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( result ).isEqualTo( List.of( "r0", "r1", "r2", "r3", "r4" ) );
    }

    @Test
    void testPartialStream()
    {
        List<String> result  = inMegaRxTx( tx ->
        {
            RxResult statementResult = tx.run( "UNWIND range(0, 4) AS i RETURN 'r' + i as A" );

            return Flux.from( statementResult.records() )
                    .limitRequest( 2 )
                    .collectList()
                    .block()
                    .stream()
                    .map( r -> r.get( "A" ).asString() )
                    .collect( Collectors.toList() );
        } );

        assertThat( result ).isEqualTo( List.of( "r0", "r1" ) );
    }

    private <T> T inMegaTx( Function<Transaction,T> workload )
    {
        try ( var session = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ) )
        {
            return session.writeTransaction( workload::apply );
        }
    }

    private <T> T inMegaRxTx( Function<RxTransaction,T> workload )
    {
        var session = clientDriver.rxSession( SessionConfig.builder().withDatabase( "mega" ).build() );
        try
        {
            RxTransaction tx = Mono.from( session.beginTransaction() ).block();
            try
            {
                return workload.apply( tx );
            }
            finally
            {
                Mono.from( tx.rollback() ).block();
            }
        }
        finally
        {
            Mono.from( session.close() ).block();
        }
    }
}
