/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.utils.DriverUtils;
import com.neo4j.utils.TestFabric;
import com.neo4j.utils.TestFabricFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.summary.ResultSummary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.neo4j.internal.helpers.Strings.joinAsLines;

class CommandsRoutingTest
{

    private static Driver clientDriver;
    private static TestFabric testFabric;
    private static DriverUtils mega = new DriverUtils( "mega" );
    private static DriverUtils neo4j = new DriverUtils( "neo4j" );
    private static DriverUtils system = new DriverUtils( "system" );

    @BeforeAll
    static void beforeAll()
    {
        testFabric = new TestFabricFactory()
                .withFabricDatabase( "mega" )
                .withShards( "myGraph" )
                .build();

        clientDriver = testFabric.routingClientDriver();

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
        testFabric.close();
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
                    "SHOW INDEXES"
            );
            return tx.run( query ).list();
        } );
        assertThat( names ).extracting( stringColumn( "name" ) ).containsExactly( "myIndex" );

        r = inMegaTx( tx ->
        {
            var query = joinAsLines(
                    "USE mega.myGraph",
                    "CREATE INDEX myIndex IF NOT EXISTS FOR (n:Person) ON (n.age)"
            );
            return tx.run( query ).consume();
        } );
        assertThat( r.counters().indexesAdded() ).isEqualTo( 0 );

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
                    "SHOW INDEXES YIELD *"
            );
            return tx.run( query ).list();
        } );
        assertThat( names ).isEmpty();
    }

    @ParameterizedTest
    @ValueSource( strings = {"0", "49", "50", "101"} )
    void testIndexManagementManyIndexes( String numberOfIndexes )
    {
        // GIVEN
        int indexes = Integer.parseInt( numberOfIndexes );
        var expectedLabels = new String[indexes];
        for ( int i = 1; i <= indexes; i++ )
        {
            expectedLabels[i - 1] = String.format( "IxLabel%d", i );
        }

        inNeo4jTx( tx ->
                   {
                       for ( String label : expectedLabels )
                       {
                           tx.run( String.format( "CREATE INDEX %s IF NOT EXISTS FOR (n:%s) ON (n.id)", label, label ) );
                       }
                       return null;
                   } );

        // WHEN
        List<Record> r = inNeo4jTx( tx -> tx.run( "SHOW ALL INDEXES" ).list() );

        // THEN
        assertThat( r.size() ).isEqualTo( indexes );
        List<String> indexNames = r.stream().map( row -> row.get( "name" ).asString() ).collect( Collectors.toList() );
        assertThat( indexNames ).containsExactlyInAnyOrder( expectedLabels );

        inNeo4jTx( tx ->
                   {
                       for ( String label : expectedLabels )
                       {
                           tx.run( String.format( "DROP INDEX %s IF EXISTS", label ) );
                       }
                       return null;
                   } );
    }

    @Test
    void testConstraintManagementOnLocal()
    {
        ResultSummary r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "USE `my-db`",
                    "CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT n.name IS UNIQUE"
            );
            return tx.run( query ).consume();
        } );

        assertThat( r.counters().constraintsAdded() ).isEqualTo( 1 );
        var names = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "USE `my-db`",
                    "SHOW CONSTRAINTS VERBOSE"
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

        assertThat( r.counters().constraintsRemoved() ).isEqualTo( 1 );

        names = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "USE `my-db`",
                    "SHOW CONSTRAINTS"
            );
            return tx.run( query ).list();
        } );
        assertThat( names ).isEmpty();

        r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "USE `my-db`",
                    "DROP CONSTRAINT myConstraint IF EXISTS"
            );
            return tx.run( query ).consume();
        } );

        assertThat( r.counters().constraintsRemoved() ).isEqualTo( 0 );
    }

    @Test
    void testCreateConstraintFailOnFabric()
    {
        assertThat( catchThrowable(() -> inMegaTx( tx ->
                tx.run( "CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS NOT NULL" ).consume()
        ) ) ).hasMessageContaining( "Schema operations are not allowed for user '' with FULL restricted to ACCESS." );

        assertThat( catchThrowable( () -> inNeo4jTx( tx ->
                tx.run( joinAsLines( "USE mega", "CREATE CONSTRAINT myConstraint ON (n:Person) ASSERT (n.name) IS NOT NULL" ) ).consume()
        ) ) ).hasMessageContaining( "Schema operations are not allowed for user '' with FULL restricted to ACCESS." );
    }

    // Administration command tests

    @Test
    void testDatabaseManagement()
    {
        ResultSummary r = inNeo4jTx( tx ->
        {
            var query = joinAsLines(
                    "CREATE DATABASE foo"
            );
            return tx.run( query ).consume();
        } );

        assertThat( r.counters().systemUpdates() ).isEqualTo( 1 );
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

        assertThat( r.counters().systemUpdates() ).isEqualTo( 1 );
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
        assertThat( r.get( 0 ).keys() ).containsExactly( "user", "roles", "passwordChangeRequired", "suspended");
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
        ) ) ).hasMessageContaining( "The `USE` clause is not required for Administration Commands. " +
                "Retry your query omitting the `USE` clause and it will be routed automatically." );
    }

    // Multiple statement types tests

    private static class Statement
    {
        final String statement;
        final String type;

        private Statement( String statement, String type )
        {
            this.statement = statement;
            this.type = type;
        }

        @Override
        public String toString()
        {
            return statement + " (" + type + ")";
        }
    }

    private static final Statement readQuery = new Statement( "RETURN 1", "Read query" );
    private static final Statement readProcedure = new Statement( "CALL db.constraints()", "Read query" );
    private static final Statement writeQuery = new Statement( "MATCH (n:Unknown) SET n.x = 1", "Write query" );
    private static final Statement schemaCreate = new Statement( "CREATE INDEX myIndex IF NOT EXISTS FOR (n:Label) ON (n.prop)", "Schema modification" );
    private static final Statement admin = new Statement( "SHOW DATABASES", "Administration command" );

    private static Stream<Arguments> validStatementTypePairs()
    {
        return Stream.of(
                Arguments.of( readQuery, readQuery ),
                Arguments.of( writeQuery, writeQuery ),
                Arguments.of( readProcedure, readProcedure ),

                Arguments.of( readQuery, writeQuery ),
                Arguments.of( writeQuery, readQuery ),

                Arguments.of( readProcedure, writeQuery ),
                Arguments.of( writeQuery, readProcedure ),

                Arguments.of( readQuery, schemaCreate ),
                Arguments.of( schemaCreate, readQuery ),

                Arguments.of( readProcedure, schemaCreate ),
                Arguments.of( schemaCreate, readProcedure ),

                Arguments.of( schemaCreate, schemaCreate ),
                Arguments.of( admin, admin )
        );
    }

    @ParameterizedTest
    @MethodSource( "validStatementTypePairs" )
    void testValidStatementTypePairs( Statement first, Statement second )
    {
        doInNeo4jTx( tx ->
                     {
                         tx.run( first.statement ).consume();
                         tx.run( second.statement ).consume();
                         tx.rollback();
                     } );
    }

    private static Stream<Arguments> invalidStatementTypePairs()
    {
        return Stream.of(
                Arguments.of( readQuery, admin ),
                Arguments.of( admin, readQuery ),

                Arguments.of( readProcedure, admin ),
                Arguments.of( admin, readProcedure ),

                Arguments.of( writeQuery, schemaCreate ),
                Arguments.of( schemaCreate, writeQuery ),

                Arguments.of( writeQuery, admin ),
                Arguments.of( admin, writeQuery ),

                Arguments.of( schemaCreate, admin ),
                Arguments.of( admin, schemaCreate )
        );
    }

    @ParameterizedTest
    @MethodSource( "invalidStatementTypePairs" )
    void testInvalidStatementTypePairsShouldFail( Statement first, Statement second )
    {
        doInNeo4jTx( tx ->
        {
            tx.run( first.statement ).consume();
            assertThat( catchThrowable( () ->
                    tx.run( second.statement ).consume()
            ) ).hasMessageContaining( String.format( "Tried to execute %s after executing %s", second.type, first.type ) );
            tx.rollback();
        } );
    }

    private static Stream<Arguments> invalidStatementTypeTriplets()
    {
        return Stream.of(
                // Upgraded to write query
                Arguments.of( readQuery, writeQuery, schemaCreate, writeQuery.type ),
                Arguments.of( readQuery, writeQuery, admin, writeQuery.type ),
                // Stays as write query
                Arguments.of( writeQuery, readQuery, schemaCreate, writeQuery.type ),
                Arguments.of( writeQuery, readQuery, admin, writeQuery.type ),
                // Upgraded to schema
                Arguments.of( readQuery, schemaCreate, writeQuery, schemaCreate.type ),
                Arguments.of( readQuery, schemaCreate, admin, schemaCreate.type )
        );
    }

    @ParameterizedTest
    @MethodSource( "invalidStatementTypeTriplets" )
    void testInvalidStatementTypeTripletsShouldFail( Statement first, Statement second, Statement third, String typeInErrorMsg )
    {
        doInNeo4jTx( tx ->
                     {
                         tx.run( first.statement ).consume();
                         tx.run( second.statement ).consume();
                         assertThat( catchThrowable( () ->
                                tx.run( third.statement ).consume()
                         ) ).hasMessageContaining( String.format( "Tried to execute %s after executing %s", third.type, typeInErrorMsg ) );
                         tx.rollback();
                     } );
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
}
