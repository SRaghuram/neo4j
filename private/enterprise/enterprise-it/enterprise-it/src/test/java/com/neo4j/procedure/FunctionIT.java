/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure;

import com.neo4j.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.jar.JarBuilder;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.procedure.Mode.WRITE;

@TestDirectoryExtension
class FunctionIT
{
    @Inject
    private TestDirectory plugins;

    private static final List<Exception> exceptionsInFunction = Collections.synchronizedList( new ArrayList<>() );
    private static final ScheduledExecutorService jobs = Executors.newScheduledThreadPool( 5 );

    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @AfterAll
    static void cleanUp()
    {
        jobs.shutdown();
    }

    @Test
    void shouldGiveNiceErrorMessageOnWrongStaticType()
    {
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        //Make sure argument here is not auto parameterized away as that will drop all type information on the floor
                        tx.execute( "RETURN com.neo4j.procedure.simpleArgument('42')" );
                    }
                } );
        assertThat( exception ).hasMessageStartingWith( "Type mismatch: expected Integer but was String (line 1, column 43 (offset: 42))" );
    }

    @Test
    void shouldGiveNiceErrorMessageWhenNoArguments()
    {
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        tx.execute( "RETURN com.neo4j.procedure.simpleArgument()" );
                    }
                } );
        assertThat( normalizeString( exception.getMessage() ) ).startsWith( format( "Function call does not provide the " +
                "required number of arguments: expected 1 got 0.%n%n" +
                "Function com.neo4j.procedure.simpleArgument has signature: " +
                "com.neo4j.procedure.simpleArgument(someValue :: INTEGER?) :: INTEGER?%n" +
                "meaning that it expects 1 argument of type INTEGER? (line 1, column 8 (offset: 7))") );
    }

    @Test
    void shouldShowDescriptionWhenMissingArguments()
    {
        // When
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        tx.execute( "RETURN com.neo4j.procedure.nodeWithDescription()" );
                    }
                } );
        assertThat( normalizeString( exception.getMessage() ) ).startsWith( format( "Function call does not provide the " +
                "required number of arguments: expected 1 got 0.%n%n" +
                "Function com.neo4j.procedure.nodeWithDescription has signature: " +
                "com.neo4j.procedure.nodeWithDescription(someValue :: NODE?) :: NODE?%n" +
                "meaning that it expects 1 argument of type NODE?%n" +
                "Description: This is a description (line 1, column 8 (offset: 7))" ) );
    }

    @Test
    void shouldCallDelegatingFunction()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.delegatingFunction($name) AS someVal",
                    map( "name", 43L ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 43L ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallRecursiveFunction()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.recursiveSum($order) AS someVal", map( "order", 10L ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 55L ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallFunctionWithGenericArgument()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute(
                    "RETURN com.neo4j.procedure.genericArguments([ ['graphs'], ['are'], ['everywhere']], " +
                    "[ [[1, 2, 3]], [[4, 5]]] ) AS someVal" );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 5L ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallFunctionWithMapArgument()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.mapArgument({foo: 42, bar: 'hello'}) AS someVal" );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 2L ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallFunctionWithMapArgumentContainingNullFromParameter()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.mapArgument({foo: $p}) AS someVal", map("p", null) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 1L ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallFunctionWithNull()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.mapArgument(null) AS someVal" );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 0L ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallFunctionWithNullFromParameter()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute(
                    "RETURN com.neo4j.procedure.mapArgument($p) AS someVal", map("p", null) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 0L ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallFunctionWithNodeReturn()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = tx.createNode().getId();

            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.node($id) AS node", map( "id", nodeId ) );

            // Then
            Node node = (Node) res.next().get( "node" );
            assertThat( node.getId() ).isEqualTo( nodeId );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldGiveHelpfulErrorOnMissingFunction()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> transaction.execute( "RETURN org.someFunctionThatDoesNotExist()" ) );
            assertThat( exception ).hasMessageStartingWith( format(
                    "Unknown function 'org.someFunctionThatDoesNotExist' (line 1, column 8 (offset: 7))" + "%n" +
                            "\"RETURN org.someFunctionThatDoesNotExist()" ) );
        }
    }

    @Test
    void shouldGiveHelpfulErrorOnExceptionMidStream()
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "RETURN com.neo4j.procedure.throwsExceptionInStream()" );

            // When
            QueryExecutionException exception = assertThrows( QueryExecutionException.class, result::next );
            assertThat( exception ).hasMessage(
                    "Failed to invoke function `com.neo4j.procedure.throwsExceptionInStream`: Caused by: java.lang" +
                            ".RuntimeException: Kaboom" );
        }
    }

    @Test
    void shouldShowCauseOfError()
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction tx = db.beginTx() )
        {
            // When
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( "RETURN com.neo4j.procedure.indexOutOfBounds()" ).next() );
            assertThat( exception ).hasMessageStartingWith(
                    "Failed to invoke function `com.neo4j.procedure.indexOutOfBounds`: Caused by: java.lang" +
                            ".ArrayIndexOutOfBoundsException" );
        }
    }

    @Test
    void shouldCallFunctionWithAccessToDB()
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label( "Person" ) ).setProperty( "name", "Buddy Holly" );
            tx.commit();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            Result res = tx.execute(
                    "RETURN com.neo4j.procedure.listCoolPeopleInDatabase() AS cool" );

            assertEquals( res.next().get( "cool" ), singletonList( "Buddy Holly" ) );
        }
    }

    @Test
    void shouldLogLikeThereIsNoTomorrow()
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        managementService.shutdown();
        managementService = new TestDatabaseManagementServiceBuilder().setInternalLogProvider( logProvider ).setUserLogProvider( logProvider ).impermanent()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.homeDir().toPath().toAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false ).build();
        db = managementService.database( DEFAULT_DATABASE_NAME );

        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result res = tx.execute( "RETURN com.neo4j.procedure.logAround()" );
            while ( res.hasNext() )
            { res.next(); }
        }

        // Then
        assertThat( logProvider ).forClass( GlobalProcedures.class )
                .forLevel( DEBUG ).containsMessages( "1" )
                .forLevel( INFO ).containsMessages( "2" )
                .forLevel( WARN ).containsMessages( "3" )
                .forLevel( ERROR ).containsMessages( "4" );
    }

    @Test
    void shouldDenyReadOnlyFunctionToPerformWrites()
    {
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        tx.execute( "RETURN com.neo4j.procedure.readOnlyTryingToWrite()" ).next();
                    }
                } );
        assertThat( exception ).hasMessageContaining( "Create node with labels '' is not allowed" );
    }

    @Test
    void shouldNotBeAbleToCallWriteProcedureThroughReadFunction()
    {
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        tx.execute( "RETURN com.neo4j.procedure.readOnlyCallingWriteProcedure()" ).next();
                    }
                } ) ;
        assertThat( exception ).hasMessageContaining( "Create node with labels '' is not allowed" );
    }

    @Test
    void shouldDenyReadOnlyFunctionToPerformSchema()
    {
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        // When
                        tx.execute( "RETURN com.neo4j.procedure.readOnlyTryingToWriteSchema()" ).next();
                    }
                } );
        assertThat( exception ).hasMessageContaining( "Schema operations are not allowed" );
    }

    @Test
    void shouldCoerceLongToDoubleAtRuntimeWhenCallingFunction()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.squareDouble($value) AS result", map( "value", 4L ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "result", 16.0d ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCoerceListOfNumbersToDoublesAtRuntimeWhenCallingFunction()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.avgNumberList($param) AS result",
                    map( "param", Arrays.<Number>asList( 1L, 2L, 3L ) ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "result", 2.0d ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCoerceListOfMixedNumbers()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.avgDoubleList([$long, $double]) AS result",
                    map( "long", 1L, "double", 2.0d ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "result", 1.5d ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCoerceDoubleToLongAtRuntimeWhenCallingFunction()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.squareLong($value) as someVal", map( "value", 4L ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 16L ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldBeAbleToPerformWritesOnNodesReturnedFromReadOnlyFunction()
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = tx.createNode().getId();
            Node node = Iterators.single(
                    tx.execute( "RETURN com.neo4j.procedure.node($id) AS node", map( "id", nodeId ) ).columnAs(
                            "node" ) );
            node.setProperty( "name", "Stefan" );
            tx.commit();
        }
    }

    @Test
    void shouldBeAbleToWriteAfterCallingReadOnlyFunction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "RETURN com.neo4j.procedure.simpleArgument(12)" ).close();
            tx.createNode();
        }
    }

    @Test
    void shouldPreserveSecurityContextWhenSpawningThreadsCreatingTransactionInFunctions() throws Throwable
    {
        // given
        Runnable doIt = () ->
        {
            try ( Transaction transaction = db.beginTx() )
            {
                try ( Result result = transaction.execute( "RETURN com.neo4j.procedure.unsupportedFunction()" ) )
                {
                    result.resultAsString();
                }
                transaction.commit();
            }
        };

        int numThreads = 10;
        Thread[] threads = new Thread[numThreads];
        for ( int i = 0; i < numThreads; i++ )
        {
            threads[i] = new Thread( doIt );
        }

        // when
        for ( int i = 0; i < numThreads; i++ )
        {
            threads[i].start();
        }

        for ( int i = 0; i < numThreads; i++ )
        {
            threads[i].join();
        }

        // Then
        Predicates.await( () -> exceptionsInFunction.size() >= numThreads, 5, TimeUnit.SECONDS );

        for ( Exception exceptionInFunction : exceptionsInFunction )
        {
            assertThat( exceptionInFunction ).isInstanceOf( AuthorizationViolationException.class )
                    .hasMessageStartingWith( "Create node with labels '' is not allowed" );
        }

        try ( Transaction transaction = db.beginTx() )
        {
            try ( Result result = transaction.execute( "MATCH () RETURN count(*) as n" ) )
            {
                assertThat( result.hasNext() ).isTrue();
                while ( result.hasNext() )
                {
                    assertThat( result.next().get( "n" ) ).isEqualTo( 0L );
                }
            }
            transaction.commit();
        }
    }

    @Test
    void shouldBeAbleToUseFunctionCallWithPeriodicCommit() throws Exception
    {
        // GIVEN
        String[] lines = IntStream.rangeClosed( 1, 100 )
                .boxed()
                .map( i -> Integer.toString( i ) )
                .toArray( String[]::new );
        String url = createCsvFile( lines );

        //WHEN
        long value = db.executeTransactionally( "USING PERIODIC COMMIT 1 " + "LOAD CSV FROM '" + url + "' AS line " +
                        "CREATE (n {prop: com.neo4j.procedure.simpleArgument(toInteger(line[0]))}) " + "RETURN n.prop", emptyMap(), result ->
                {
                    long counter = 1;
                    while ( result.hasNext() )
                    {
                        var row = result.next();
                        assertThat( row.get( "n.prop" ) ).isEqualTo( counter++ );
                    }
                    return counter;
                } );
                // THEN
        assertEquals( 101L, value );
        try ( Transaction transaction = db.beginTx() )
        {
            //Make sure all the lines has been properly commited to the database.
            String[] dbContents =
                    transaction.execute( "MATCH (n) return n.prop" ).stream().map( m -> Long.toString( (long) m.get( "n.prop" ) ) ).toArray( String[]::new );
            assertThat( dbContents ).isEqualTo( lines );
            transaction.commit();
        }
    }

    @Test
    void shouldFailIfUsingPeriodicCommitWithReadOnlyQuery() throws IOException
    {
        String url = createCsvFile( "13" );

        try ( Transaction transaction = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows( QueryExecutionException.class, () -> transaction.execute(
                    "USING PERIODIC COMMIT 1 " + "LOAD CSV FROM '" + url + "' AS line " +
                            "WITH com.neo4j.procedure.simpleArgument(toInteger(line[0])) AS val " + "RETURN val" ) );
            assertThat( exception ).hasMessageStartingWith( "Cannot use periodic commit in a non-updating query (line 1, column 1 (offset: 0))" );
        }
    }

    @Test
    void shouldCallFunctionReturningPaths()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            Relationship rel = node1.createRelationshipTo( node2, RelationshipType.withName( "KNOWS" ) );

            // When
            Result res = tx.execute( "RETURN com.neo4j.procedure.nodePaths($node) AS path", map( "node", node1 ) );

            // Then
            assertTrue( res.hasNext() );
            Map<String,Object> value = res.next();
            Path path = (Path) value.get( "path" );
            assertThat( path.length() ).isEqualTo( 1 );
            assertThat( path.startNode() ).isEqualTo( node1 );
            assertThat( asList( path.relationships() ) ).isEqualTo( singletonList( rel ) );
            assertThat( path.endNode() ).isEqualTo( node2 );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldBeAbleToUseUDFForLimit()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "UNWIND range(0, 100) AS r RETURN r LIMIT com.neo4j.procedure.squareLong(2)");

            // Then
            List<Object> list =
                    Iterators.asList( res ).stream().map( m  -> m.get( "r" ) ).collect( Collectors.toList() );
            assertThat( list ).isEqualTo( Arrays.asList( 0L, 1L, 2L, 3L ) );
        }
    }

    private String createCsvFile( String... lines ) throws IOException
    {
        File file = plugins.createFile( "test" );

        try ( PrintWriter writer = FileUtils.newFilePrintWriter( file, UTF_8 ) )
        {
            for ( String line : lines )
            {
                writer.println( line );
            }
        }

        return file.toURI().toURL().toString();
    }

    @Test
    void shouldHandleAggregationFunctionInFunctionCall()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( "Person" ) );
            tx.createNode( Label.label( "Person" ) );
            assertEquals( 2L, tx.execute( "MATCH (n:Person) RETURN com.neo4j.procedure.nodeListArgument(collect(n)) AS someVal" )
                    .next()
                    .get( "someVal" ) );
        }
    }

    @Test
    void shouldHandleNullInList()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( "Person" ) );
            assertEquals( 1L, tx.execute( "MATCH (n:Person) RETURN com.neo4j.procedure.nodeListArgument([n, null]) AS someVal" )
                    .next()
                    .get( "someVal" ) );
        }
    }

    @Test
    void shouldWorkWhenUsingWithToProjectList()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( "Person" ) );
            tx.createNode( Label.label( "Person" ) );

            // When
            Result res = tx.execute(
                    "MATCH (n:Person) WITH collect(n) as persons RETURN com.neo4j.procedure.nodeListArgument(persons)" +
                    " AS someVal" );

            // THEN
            assertThat( res.next().get( "someVal" ) ).isEqualTo( 2L );
        }
    }

    @Test
    void shouldSeeEmptyGraphReadFunctionInAccessTransaction()
    {
        // Given
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.EXPLICIT, AnonymousContext.full() ) )
        {
            tx.execute( "CREATE ()" );
            tx.commit();
        }

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.EXPLICIT, AnonymousContext.access() ) )
        {
            Result result = tx.execute( "RETURN com.neo4j.procedure.nodeCount() AS count" );

            // Then
            assertThat( result.next().get( "count" ) ).isEqualTo( 0L );
            tx.commit();
        }
    }

    @Test
    void shouldCallProcedureWithAllDefaultArgument()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( "RETURN com.neo4j.procedure.defaultValues() AS result" );

            // Then
            assertThat( res.next().get( "result" ) ).isEqualTo( "a string,42,3.14,true" );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleNullAsParameter()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( "RETURN com.neo4j.procedure.defaultValues($p) AS result", map( "p", null ) );

            // Then
            assertThat( res.next().get( "result" ) ).isEqualTo( "null,42,3.14,true" );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @Test
    void shouldCallFunctionWithOneProvidedRestDefaultArgument()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( "RETURN com.neo4j.procedure.defaultValues('another string') AS result" );

            // Then
            assertThat( res.next().get( "result" ) ).isEqualTo( "another string,42,3.14,true" );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @Test
    void shouldCallFunctionWithTwoProvidedRestDefaultArgument()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( "RETURN com.neo4j.procedure.defaultValues('another string', 1337) AS result" );

            // Then
            assertThat( res.next().get( "result" ) ).isEqualTo( "another string,1337,3.14,true" );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @Test
    void shouldCallFunctionWithThreeProvidedRestDefaultArgument()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( "RETURN com.neo4j.procedure.defaultValues('another string', 1337, 2.718281828) AS result" );

            // Then
            assertThat( res.next().get( "result" ) ).isEqualTo( "another string,1337,2.72,true" );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @Test
    void shouldCallFunctionWithFourProvidedRestDefaultArgument()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( "RETURN com.neo4j.procedure.defaultValues('another string', 1337, 2.718281828, false) AS result" );

            // Then
            assertThat( res.next().get( "result" ) ).isEqualTo( "another string,1337,2.72,false" );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @Test
    void shouldCallFunctionReturningNull()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( "RETURN com.neo4j.procedure.node(-1) AS result" );

            // Then
            assertThat( res.next().get( "result" ) ).isNull();
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    /**
     * NOTE: this test tests user-defined functions added in this file {@link ClassWithFunctions}. These are not
     * built-in functions in any shape or form.
     */
    @Test
    void shouldListAllFunctions()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( "CALL dbms.functions()" );

            try ( BufferedReader reader = new BufferedReader( new InputStreamReader( FunctionIT.class.getResourceAsStream( "/misc/functions" ) ) ) )
            {
                String expected = reader.lines().collect( Collectors.joining( lineSeparator() ) );
                String actual = res.resultAsString();
                // Be aware that the text file "functions" must end with two newlines
                assertEquals( expected, actual );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Failed to read functions file." );
            }
        }
    }

    @Test
    void shouldCallFunctionWithSameNameAsBuiltIn()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( "RETURN this.is.test.only.sum([1337, 2.718281828, 3.1415]) AS result" );

            // Then
            assertThat( res.next().get( "result" ) ).isEqualTo( 1337 + 2.718281828 + 3.1415 );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @Test
    void shouldNotSupportPipelinedRuntime()
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( "CYPHER runtime=PIPELINED RETURN com.neo4j.procedure.squareLong(2) AS someVal" );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 4L ) );
            assertFalse( res.hasNext() );
            assertThat( res.getExecutionPlanDescription().getArguments().get( "runtime" ) ).isNotEqualTo( "PIPELINED" );
        }
    }

    @BeforeEach
    void setUp() throws IOException
    {
        exceptionsInFunction.clear();
        new JarBuilder().createJarFor( plugins.createFile( "myFunctions.jar" ), ClassWithFunctions.class );
        managementService = new TestDatabaseManagementServiceBuilder().impermanent()
                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.homeDir().toPath().toAbsolutePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false ).build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        if ( this.db != null )
        {
            this.managementService.shutdown();
        }
    }

    public static class ClassWithFunctions
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Transaction transaction;

        @Context
        public Log log;

        @UserFunction
        public long nodeCount()
        {
            return transaction.getAllNodes().stream().count();
        }

        @UserFunction
        public long integrationTestMe()
        {
            return 1337L;
        }

        @UserFunction
        public long simpleArgument( @Name( "someValue" ) long someValue )
        {
            return someValue;
        }

        @UserFunction
        public String defaultValues(
                @Name( value = "string", defaultValue = "a string" ) String string,
                @Name( value = "integer", defaultValue = "42" ) long integer,
                @Name( value = "float", defaultValue = "3.14" ) double aFloat,
                @Name( value = "boolean", defaultValue = "true" ) boolean aBoolean
        )
        {
            return format( Locale.ROOT, "%s,%d,%.2f,%b", string, integer, aFloat, aBoolean );
        }

        @UserFunction
        public long nodeListArgument( @Name( "nodes" ) List<Node> nodes )
        {
            long count = 0L;
            for ( Node node : nodes )
            {
                if ( node != null )
                {
                    count++;
                }
            }
            return count;
        }

        @UserFunction
        public long delegatingFunction( @Name( "someValue" ) long someValue )
        {
            return (long) transaction
                    .execute( "RETURN com.neo4j.procedure.simpleArgument($name) AS result", map( "name", someValue ) )
                    .next().get( "result" );
        }

        @UserFunction
        public long recursiveSum( @Name( "someValue" ) long order )
        {
            if ( order == 0L )
            {
                return 0L;
            }
            Long prev =
                    (Long) transaction
                            .execute( "RETURN com.neo4j.procedure.recursiveSum($order) AS someVal",
                            map( "order", order - 1 ) )
                            .next().get( "someVal" );
            return order + prev;
        }

        @UserFunction
        public long genericArguments( @Name( "strings" ) List<List<String>> stringList,
                @Name( "longs" ) List<List<List<Long>>> longList )
        {
            return stringList.size() + longList.size();
        }

        @UserFunction
        public long mapArgument( @Name( "map" ) Map<String,Object> map )
        {
            if ( map == null )
            {
                return 0;
            }
            return map.size();
        }

        @UserFunction
        public Node node( @Name( "id" ) long id )
        {
            if ( id < 0 )
            {
                return null;
            }
            return transaction.getNodeById( id );
        }

        @UserFunction
        public double squareDouble( @Name( "someValue" ) double value )
        {
            return value * value;
        }

        @UserFunction
        public double avgNumberList( @Name( "someValue" ) List<Number> list )
        {
            return list.stream().reduce( ( l, r ) -> l.doubleValue() + r.doubleValue() ).orElse( 0.0d ).doubleValue() /
                   list.size();
        }

        @UserFunction
        public double avgDoubleList( @Name( "someValue" ) List<Double> list )
        {
            return list.stream().reduce( ( l, r ) -> l + r ).orElse( 0.0d ) / list.size();
        }

        @UserFunction
        public long squareLong( @Name( "someValue" ) long value )
        {
            return value * value;
        }

        @UserFunction
        public long throwsExceptionInStream()
        {
            throw new RuntimeException( "Kaboom" );
        }

        @UserFunction
        public long indexOutOfBounds()
        {
            int[] ints = {1, 2, 3};
            return ints[4];
        }

        @UserFunction
        public List<String> listCoolPeopleInDatabase()
        {
            return transaction.findNodes( label( "Person" ) )
                    .map( node -> (String) node.getProperty( "name" ) )
                    .stream()
                    .collect( Collectors.toList() );
        }

        @UserFunction
        public long logAround()
        {
            log.debug( "1" );
            log.info( "2" );
            log.warn( "3" );
            log.error( "4" );
            return 1337L;
        }

        @UserFunction
        public Node readOnlyTryingToWrite()
        {
            return transaction.createNode();
        }

        @UserFunction
        public Node readOnlyCallingWriteFunction()
        {
            return (Node) transaction.execute( "RETURN com.neo4j.procedure.writingFunction() AS node" ).next().get(
                    "node" );
        }

        @UserFunction
        public long readOnlyCallingWriteProcedure()
        {
            transaction.execute( "CALL com.neo4j.procedure.writingProcedure()" );
            return 1337L;
        }

        @UserFunction( "this.is.test.only.sum" )
        public Number sum( @Name( "numbers" ) List<Number> numbers )
        {
            return numbers.stream().mapToDouble( Number::doubleValue ).sum();
        }

        @Procedure( mode = WRITE )
        public void writingProcedure()
        {
            transaction.createNode();
        }

        @UserFunction
        public String unsupportedFunction()
        {
            jobs.submit( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.createNode();
                    tx.commit();
                }
                catch ( Exception e )
                {
                    exceptionsInFunction.add( e );
                }
            } );

            return "why!?";
        }

        @UserFunction
        public Path nodePaths( @Name( "someValue" ) Node node )
        {
            return (Path) transaction
                    .execute( "WITH $node AS node MATCH p=(node)-[*]->() RETURN p", map( "node", node ) )
                    .next()
                    .getOrDefault( "p", null );
        }

        @Description( "This is a description" )
        @UserFunction
        public Node nodeWithDescription( @Name( "someValue" ) Node node )
        {
            return node;
        }

        @UserFunction
        public String readOnlyTryingToWriteSchema()
        {
            transaction.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
            return "done";
        }
    }

    private static String normalizeString( String value )
    {
        return value.replaceAll( "\\r?\\n", System.lineSeparator() );
    }
}
