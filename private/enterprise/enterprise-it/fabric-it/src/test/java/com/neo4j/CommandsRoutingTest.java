/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.ProxyFunctions;
import com.neo4j.utils.ShardFunctions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.exceptions.KernelException;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

@ExtendWith( FabricEverywhereExtension.class )
class CommandsRoutingTest
{

    private static Driver clientDriver;
    private static TestServer testServer;
    private static Neo4j shard;
    private static Driver shardDriver;
    private static DriverUtils mega = new DriverUtils( "mega" );
    private static DriverUtils neo4j = new DriverUtils( "neo4j" );
    private static DriverUtils system = new DriverUtils( "system" );

    @BeforeAll
    static void beforeAll() throws KernelException
    {
        shard = Neo4jBuilders.newInProcessBuilder().withProcedure( ShardFunctions.class ).build();

        var configProperties = Map.of(
                "fabric.database.name", "mega",
                "fabric.graph.0.uri", shard.boltURI().toString(),
                "fabric.graph.0.name", "myGraph",
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:0",
                "dbms.connector.bolt.enabled", "true"
        );

        var config = Config.newBuilder()
                .setRaw( configProperties )
                .build();
        testServer = new TestServer( config );

        testServer.start();

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

        shardDriver = GraphDatabase.driver(
                shard.boltURI(),
                AuthTokens.none(),
                org.neo4j.driver.Config.builder()
                        .withoutEncryption()
                        .withMaxConnectionPoolSize( 3 )
                        .build() );

        system.doInTx( clientDriver, tx ->
        {
            tx.run( "CREATE DATABASE `my-db`" );
            tx.run( "CREATE USER myUser SET PASSWORD 'password'" );
            tx.run( "CREATE ROLE myRole" );
            tx.run( "GRANT ROLE myRole TO myUser" );
            tx.run( "GRANT ACCESS ON DEFAULT DATABASE TO myRole" );
            tx.commit();
        } );
    }

    @AfterAll
    static void afterAll()
    {
        List.<Runnable>of(
                () -> testServer.stop(),
                () -> clientDriver.close(),
                () -> shardDriver.close(),
                () -> shard.close()
        ).parallelStream().forEach( Runnable::run );
    }

    // Index and Constraint tests

    @Test
    void testIndexManagementOnRemote()
    {
        ResultSummary r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "USE mega.myGraph",
                    "CREATE INDEX myIndex FOR (n:Person) ON (n.name)"
            );
            return tx.run( query ).consume();
        } );

        assertThat( r.counters().indexesAdded() ).isEqualTo( 1 );
        var names = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "USE mega.myGraph",
                    "CALL db.indexes() YIELD name RETURN *"
            );
            return tx.run( query ).list();
        } );
        assertThat( names ).extracting( stringColumn( "name" ) ).containsExactly( "myIndex" );

        r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "USE mega.myGraph",
                    "DROP INDEX myIndex"
            );
            return tx.run( query ).consume();
        } );

        assertThat( r.counters().indexesRemoved() ).isEqualTo( 1 );
        names = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "USE mega.myGraph",
                    "CALL db.indexes() YIELD name RETURN *"
            );
            return tx.run( query ).list();
        } );
        assertThat( names ).isEmpty();
    }

    @Test
    void testConstraintManagementOnLocal() // TODO: re-add check for counters when counting on Local
    {
        ResultSummary r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "USE `my-db`",
                    "CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT n.name IS UNIQUE"
            );
            return tx.run( query ).consume();
        } );

//        assertThat( r.counters().constraintsAdded() ).isEqualTo( 1 );
        var names = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "USE `my-db`",
                    "CALL db.constraints() YIELD name RETURN *"
            );
            return tx.run( query ).list();
        } );
        assertThat( names ).extracting( stringColumn( "name" ) ).containsExactly( "myConstraint" );

        r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "USE `my-db`",
                    "DROP CONSTRAINT myConstraint"
            );
            return tx.run( query ).consume();
        } );

//        assertThat( r.counters().constraintsRemoved() ).isEqualTo( 1 );
        names = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "USE `my-db`",
                    "CALL db.constraints() YIELD name RETURN *"
            );
            return tx.run( query ).list();
        } );
        assertThat( names ).isEmpty();
    }

    @Test
    void testCreateConstraintFailOnFabric()
    {
        assertThat( catchThrowable(() -> inMegaTx( tx ->
                tx.run( "CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT exists(n.name)" ).consume()
        ) ) ).hasMessageContaining( "Schema operations are not allowed for user '' with FULL restricted to ACCESS." );

        assertThat( catchThrowable( () -> inNeo4jTx( tx ->
                tx.run( joinAsLines( "USE mega", "CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT exists(n.name)" ) ).consume()
        ) ) ).hasMessageContaining( "Schema operations are not allowed for user '' with FULL restricted to ACCESS." );
    }

    // Administration command tests

    @Test
    void testDatabaseManagement() // TODO: re-add check for counters when counting on Local
    {
        ResultSummary r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "CREATE DATABASE foo"
            );
            return tx.run( query ).consume();
        } );

//        assertThat( r.counters().systemUpdates() ).isEqualTo( 1 );
        system.doInTx( clientDriver, tx -> {
            var res = tx.run( "SHOW DATABASE foo" ).list();
            assertThat( res.size() ).isEqualTo( 1 );
            assertThat( res ).extracting( stringColumn( "name" ) ).containsExactly( "foo" );
        } );

        r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "DROP DATABASE foo"
            );
            return tx.run( query ).consume();
        } );

//        assertThat( r.counters().systemUpdates() ).isEqualTo( 1 );
        system.doInTx( clientDriver, tx -> {
            var res = tx.run( "SHOW DATABASE foo" ).list();
            assertThat( res.size() ).isEqualTo( 0 );
        } );
    }

    @Test
    void testShowUsers()
    {
        List<Record> r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "SHOW USERS"
            );
            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isGreaterThanOrEqualTo( 1 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "user", "roles", "passwordChangeRequired", "suspended" );
    }

    @Test
    void testShowRoles()
    {
        List<Record> r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "SHOW ROLES WITH USERS"
            );
            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isGreaterThanOrEqualTo( 1 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "role", "member" );
    }

    @Test
    void testShowAllPrivileges()
    {
        List<Record> r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "SHOW PRIVILEGES"
            );
            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isGreaterThanOrEqualTo( 1 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "access", "action", "resource", "graph", "segment", "role" );
    }

    @Test
    void testShowRolePrivileges()
    {
        List<Record> r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "SHOW ROLE myRole PRIVILEGES"
            );
            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isGreaterThanOrEqualTo( 1 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "access", "action", "resource", "graph", "segment", "role" );
    }

    @Test
    void testShowUserPrivileges()
    {
        List<Record> r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "SHOW USER myUser PRIVILEGES"
            );
            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isGreaterThanOrEqualTo( 1 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "access", "action", "resource", "graph", "segment", "role", "user" );
    }

    @Test
    void testShowDatabases()
    {
        List<Record> r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "SHOW DATABASES"
            );
            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isGreaterThanOrEqualTo( 1 );
        assertThat( r.get( 0 ).keys() ).containsExactly( "name", "address", "role", "requestedStatus", "currentStatus", "error", "default" );
    }

    @Test
    void testShowDefaultDatabase()
    {
        List<Record> r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "SHOW DEFAULT DATABASE"
            );
            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r ).extracting( stringColumn( "name" ) ).containsExactly( "neo4j" );
        assertThat( r.get( 0 ).keys() ).containsExactly( "name", "address", "role", "requestedStatus", "currentStatus", "error" );
    }

    @Test
    void testShowDatabase()
    {
        List<Record> r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "SHOW DATABASE `my-db`"
            );
            return tx.run( query ).list();
        } );

        assertThat( r.size() ).isEqualTo( 1 );
        assertThat( r ).extracting( stringColumn( "name" ) ).containsExactly( "my-db" );
        assertThat( r.get( 0 ).keys() ).containsExactly( "name", "address", "role", "requestedStatus", "currentStatus", "error", "default" );
    }

    @Test
    void testAdministrationCommandFailWhenHavingUseClause()
    {
        assertThat( catchThrowable( () -> inNeo4jTx( tx ->
                tx.run( joinAsLines( "USE system", "CREATE ROLE foo" ) ).consume()
        ) ) ).hasMessageContaining( "The `USE` clause is not supported for Administration Commands." );
    }

    // Multiple statement types tests

    private static final String queryString = "RETURN 1";
    private static final String queryType = "Query";
    private static final String schemaStringCreate = "CREATE INDEX myIndex FOR (n:Label) ON (n.prop)";
    private static final String schemaStringDrop = "DROP INDEX myIndex";
    private static final String schemaType = "Schema modification";
    private static final String adminString = "SHOW DATABASES";
    private static final String adminType = "Administration command";

    @Test
    void testQueryFollowsQuery()
    {
        doInNeo4jTx( tx ->
        {
            tx.run( queryString ).consume();
            tx.run( queryString ).consume();
        } );
    }

    @Test
    void testSchemaFollowsSchema()
    {
        doInNeo4jTx( tx ->
        {
            tx.run( schemaStringCreate ).consume();
            tx.run( schemaStringDrop ).consume();
        } );
    }

    @Test
    void testAdminFollowsAdmin()
    {
        doInNeo4jTx( tx ->
        {
            tx.run( adminString ).consume();
            tx.run( adminString ).consume();
        } );
    }

    @ParameterizedTest
    @MethodSource( "getStatementTypeCombinations" )
    void testMixingStatementTypesShouldFail( String firstStatement, String firstType, String secondStatement, String secondType )
    {
        doInNeo4jTx( tx ->
        {
            tx.run( firstStatement ).consume();
            assertThat( catchThrowable( () ->
                    tx.run( secondStatement ).consume()
            ) ).hasMessageContaining( String.format( "Tried to execute %s after executing %s", secondType, firstType ) );
            tx.rollback();
        } );

        // Possible clean-up
        try
        {
            doInNeo4jTx( tx -> tx.run( schemaStringDrop ).consume() );
        }
        catch ( Throwable e )
        {
            // ignore
        }
    }

    // Help methods

    private <T> T inNeo4jTx( Function<Transaction, T> workload )
    {
        return neo4j.inTx( clientDriver, workload );
    }

    private void doInNeo4jTx( Consumer<Transaction> workload )
    {
        neo4j.doInTx( clientDriver, workload );
    }

    private <T> T inMegaTx( Function<Transaction, T> workload )
    {
        return mega.inTx( clientDriver, workload );
    }

    private static Function<Record,String> stringColumn( String column )
    {
        return row -> row.get( column ).asString();
    }

    private static Stream<Arguments> getStatementTypeCombinations()
    {
        return Stream.of(
                Arguments.of( queryString, queryType, schemaStringCreate, schemaType ),
                Arguments.of( queryString, queryType, adminString, adminType ),
                Arguments.of( schemaStringCreate, schemaType, adminString, adminType ),
                Arguments.of( schemaStringCreate, schemaType, queryString, queryType ),
                Arguments.of( adminString, adminType, schemaStringCreate, schemaType ),
                Arguments.of( adminString, adminType, queryString, queryType )
        );
    }
}
