/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.test.routing.FabricEverywhereExtension;
import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.summary.ResultSummary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.driver.summary.QueryType.READ_ONLY;
import static org.neo4j.driver.summary.QueryType.READ_WRITE;
import static org.neo4j.driver.summary.QueryType.SCHEMA_WRITE;
import static org.neo4j.driver.summary.QueryType.WRITE_ONLY;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

@ExtendWith( FabricEverywhereExtension.class )
class QueryExecutionTypeTest
{

    private static Driver mainDriver;
    private static TestFabric testFabric;
    private static final DriverUtils neo4j = new DriverUtils( "neo4j" );
    private static final DriverUtils fabric = new DriverUtils( "fabric" );
    private static final DriverUtils system = new DriverUtils( "system" );

    @BeforeAll
    static void beforeAll()
    {
        testFabric = new TestFabricFactory()
                .withFabricDatabase( "fabric" )
                .withShards( "extA" )
                // Un-comment to get debug log to console
                // .withLogService( new SimpleLogService( new StdoutLogProvider()  )
                .build();

        mainDriver = testFabric.routingClientDriver();

        doInTx( mainDriver, system, tx ->
        {
            tx.run( "CREATE DATABASE intA" ).consume();
            tx.run( "CREATE DATABASE intB" ).consume();
            tx.commit();
        } );
    }

    @AfterAll
    static void afterAll()
    {
        testFabric.close();
    }

    @Test
    void localReadOnly()
    {
        var query = joinAsLines( "MATCH (n) RETURN n" );

        assertThat( summary( neo4j, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( READ_ONLY );
    }

    @Test
    void localWriteOnly()
    {
        var query = joinAsLines( "CREATE ()" );

        assertThat( summary( neo4j, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( WRITE_ONLY );
    }

    @Test
    void localReadWrite()
    {
        var query = joinAsLines( "MATCH (n) CREATE () RETURN n" );

        assertThat( summary( neo4j, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( READ_WRITE );
    }

    @Test
    void localSchemaWrite()
    {
        var query = joinAsLines( "CREATE INDEX FOR (n:Foo) ON (n.p)" );

        assertThat( summary( neo4j, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( SCHEMA_WRITE );
    }

    @Test
    void remoteReadOnly()
    {
        var query = joinAsLines( "USE fabric.extA MATCH (n) RETURN n" );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( READ_ONLY );
    }

    @Test
    @Disabled( "query type for remote queries is not forwarded properly" )
    void remoteWriteOnly()
    {
        var query = joinAsLines( "USE fabric.extA CREATE ()" );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( WRITE_ONLY );
    }

    @Test
    void remoteReadWrite()
    {
        var query = joinAsLines( "USE fabric.extA MATCH (n) CREATE () RETURN n" );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( READ_WRITE );
    }

    @Test
    @Disabled( "query type for remote queries is not forwarded properly" )
    void remoteSchemaWrite()
    {
        var query = joinAsLines( "USE fabric.extA CREATE INDEX FOR (n:Foo) ON (n.p)" );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( SCHEMA_WRITE );
    }

    @Test
    void unionReadOnly()
    {
        var query = joinAsLines(
                "USE intA MATCH (n) RETURN n",
                "  UNION",
                "USE fabric.extA MATCH (n) RETURN n"
        );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( READ_ONLY );
    }

    @Test
    void unionReadWrite()
    {
        var query = joinAsLines(
                "USE intA MATCH (n) RETURN n",
                "  UNION",
                "USE fabric.extA MATCH (n) CREATE () RETURN n"
        );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( READ_WRITE );
    }

    @Test
    @Disabled( "query type for remote queries is not forwarded properly" )
    void unionWriteOnly()
    {
        var query = joinAsLines(
                "USE fabric.extA CREATE ()",
                "  UNION",
                "USE fabric.extA CREATE ()"
        );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( WRITE_ONLY );
    }

    @Test
    void localUnionReadOnly()
    {
        var query = joinAsLines(
                "USE intA MATCH (n) RETURN n",
                "  UNION",
                "USE intB MATCH (n) RETURN n"
        );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( READ_ONLY );
    }

    @Test
    void localUnionReadWrite()
    {
        var query = joinAsLines(
                "USE intA MATCH (n) RETURN n",
                "  UNION",
                "USE intB MATCH (n) CREATE () RETURN n"
        );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( READ_WRITE );
    }

    @Test
    void localUnionWriteOnly()
    {
        var query = joinAsLines(
                "USE intB CREATE ()",
                "  UNION",
                "USE intB CREATE ()"
        );

        assertThat( summary( fabric, query ) )
                .extracting( ResultSummary::queryType )
                .isEqualTo( WRITE_ONLY );
    }

    private static ResultSummary summary( DriverUtils context, String query )
    {
        return inTx( mainDriver, context, tx -> tx.run( query ).consume() );
    }

    private static <T> T inTx( Driver driver, DriverUtils driverUtils, Function<Transaction,T> workload )
    {
        return driverUtils.inTx( driver, workload );
    }

    private static void doInTx( Driver driver, DriverUtils driverUtils, Consumer<Transaction> workload )
    {
        driverUtils.doInTx( driver, workload );
    }
}
