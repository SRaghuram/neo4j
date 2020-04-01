/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.ProxyFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Transaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.monitoring.Monitors;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class FabricFrontEndTest
{

    private static Driver clientDriver;
    private static TestServer testServer;
    private static DriverUtils driverUtils;
    private static DummyTimingCompilationTracerEventListener tracerListener;

    static class DummyTimingCompilationTracerEventListener implements TimingCompilationTracer.EventListener
    {
        List<String> queries = new ArrayList<>();
        List<TimingCompilationTracer.QueryEvent> events = new ArrayList<>();

        @Override
        public void startQueryCompilation( String query )
        {
            queries.add( query );
        }

        @Override
        public void queryCompiled( TimingCompilationTracer.QueryEvent event )
        {
            events.add( event );
        }

        public void clear()
        {
            queries.clear();
            events.clear();
        }
    }

    @BeforeAll
    static void beforeAll() throws KernelException
    {
        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true"
        );

        var config = Config.newBuilder()
                           .setRaw( configProperties )
                           .build();
        testServer = new TestServer( config );
        testServer.start();

        tracerListener = new DummyTimingCompilationTracerEventListener();
        testServer.getDependencies().resolveDependency( Monitors.class )
                  .addMonitorListener( tracerListener );

        var globalProceduresRegistry = testServer.getDependencies().resolveDependency( GlobalProceduresRegistry.class );
        globalProceduresRegistry
                .registerFunction( ProxyFunctions.class );
        globalProceduresRegistry
                .registerProcedure( ProxyFunctions.class );

        clientDriver = GraphDatabase.driver(
                testServer.getBoltRoutingUri(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                                       .withoutEncryption()
                                       .withMaxConnectionPoolSize( 3 )
                                       .build() );

        driverUtils = new DriverUtils( "mega" );
    }

    @BeforeEach
    void beforeEach()
    {
        tracerListener.clear();
    }

    @AfterAll
    static void afterAll()
    {
        List.<Runnable>of(
                () -> testServer.stop(),
                () -> clientDriver.close()
        ).parallelStream().forEach( Runnable::run );
    }

    @Test
    void testDeprecationNotification()
    {
        var notifications = inMegaTx( tx -> tx.run( "explain MATCH ()-[rs*]-() RETURN rs" ).consume().notifications() );

        assertThat( notifications.size() ).isEqualTo( 1 );
        assertThat( notifications.get( 0 ).code() ).isEqualTo( "Neo.ClientNotification.Statement.FeatureDeprecationWarning" );
        assertThat( notifications.get( 0 ).description() ).startsWith( "Binding relationships" );
    }

    @Test
    void testCompilationTracing()
    {
        inMegaTx( tx -> tx.run( "RETURN 1" ).consume() );

        assertThat( tracerListener.queries ).contains( "RETURN 1" );
        assertThat( tracerListener.events ).extracting( TimingCompilationTracer.QueryEvent::query ).contains( "RETURN 1" );
    }

    private <T> T inMegaTx( Function<Transaction,T> workload )
    {
        return driverUtils.inTx( clientDriver, workload );
    }
}
