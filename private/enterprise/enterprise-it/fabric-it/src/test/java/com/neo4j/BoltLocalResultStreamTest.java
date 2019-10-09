/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.CustomFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.internal.SessionConfig;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Flux;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Mono;
import org.neo4j.driver.reactive.RxStatementResult;
import org.neo4j.driver.reactive.RxTransaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class BoltLocalResultStreamTest
{
    private static Driver clientDriver;
    private static TestServer testServer;

    @BeforeAll
    static void setUp() throws KernelException
    {

        PortUtils.Ports ports = PortUtils.findFreePorts();
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", "somewhere:6666",
                "fabric.routing.servers", "localhost:" + ports.bolt,
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + ports.bolt,
                "dbms.connector.bolt.enabled", "true"
        );

        Config config = Config.newBuilder().setRaw( configProperties ).build();
        testServer = new TestServer( config );

        testServer.start();

        testServer.getDependencies().resolveDependency( GlobalProceduresRegistry.class )
                .registerFunction( CustomFunctions.class );

        clientDriver = GraphDatabase.driver(
                "neo4j://localhost:" + ports.bolt,
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
        List<String> result;
        try ( Transaction tx = clientDriver.session( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() )
        {
            result = tx.run( "UNWIND range(0, 4) AS i RETURN 'r' + i as A" ).stream()
                    .map( r -> r.get( "A" ).asString() )
                    .collect( Collectors.toList() );
            tx.success();
        }

        assertThat( result, equalTo( List.of( "r0", "r1", "r2", "r3", "r4" ) ) );
    }

    @Test
    void testRxResultStream()
    {
        RxTransaction tx = Mono.from( clientDriver.rxSession( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() ).block();

        try
        {
            RxStatementResult statementResult = tx.run( "UNWIND range(0, 4) AS i RETURN 'r' + i as A" );

            List<String> result =
                    Flux.from( statementResult.records() )
                            .limitRate( 1 )
                            .collectList()
                            .block()
                            .stream()
                            .map( r -> r.get( "A" ).asString() )
                            .collect( Collectors.toList() );

            assertThat( result, equalTo( List.of( "r0", "r1", "r2", "r3", "r4" ) ) );
        }
        finally
        {
            Mono.from( tx.rollback() ).block();
        }
    }

    @Test
    void testPartialStream()
    {
        RxTransaction tx = Mono.from( clientDriver.rxSession( SessionConfig.builder().withDatabase( "mega" ).build() ).beginTransaction() ).block();

        try
        {
            RxStatementResult statementResult = tx.run( "UNWIND range(0, 4) AS i RETURN 'r' + i as A" );

            List<String> result = Flux.from( statementResult.records() )
                    .limitRequest( 2 )
                    .collectList()
                    .block()
                    .stream()
                    .map( r -> r.get( "A" ).asString() )
                    .collect( Collectors.toList() );

            assertThat( result, equalTo( List.of( "r0", "r1" ) ) );
        }
        finally
        {
            Mono.from( tx.rollback() ).block();
        }
    }
}
