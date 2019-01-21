/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure;

import com.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.proc.GlobalProcedures;
import org.neo4j.kernel.impl.proc.JarBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;
import static org.neo4j.procedure.StringMatcherIgnoresNewlines.containsStringIgnoreNewlines;

@ExtendWith( TestDirectoryExtension.class )
public class ProcedureIT
{
    @Inject
    private TestDirectory plugins;

    private static List<Exception> exceptionsInProcedure = Collections.synchronizedList( new ArrayList<>() );
    private GraphDatabaseService db;
    static boolean[] onCloseCalled;

    @BeforeEach
    void setUp() throws IOException
    {
        exceptionsInProcedure.clear();
        new JarBuilder().createJarFor( plugins.createFile( "myProcedures.jar" ), ClassWithProcedures.class );
        new JarBuilder().createJarFor( plugins.createFile( "myFunctions.jar" ), ClassWithFunctions.class );
        db = new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig( plugin_dir, plugins.directory().getAbsolutePath() ).setConfig(
                GraphDatabaseSettings.record_id_batch_size, "1" ).newGraphDatabase();
        onCloseCalled = new boolean[2];
    }

    @AfterEach
    void tearDown()
    {
        if ( this.db != null )
        {
            this.db.shutdown();
        }
    }

    @Test
    void shouldCallProcedureWithParameterMap()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.simpleArgument", map( "name", 42L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 42L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithDefaultArgument()
    {
        //Given/When
        Result res = db.execute( "CALL org.neo4j.procedure.simpleArgumentWithDefault" );

        // Then
        assertThat( res.next(), equalTo( map( "someVal", 42L ) ) );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldCallYieldProcedureWithDefaultArgument()
    {
        // Given/When
        Result res = db.execute( "CALL org.neo4j.procedure.simpleArgumentWithDefault() YIELD someVal as n RETURN n + 1295 as val" );

        // Then
        assertThat( res.next(), equalTo( map( "val", 1337L ) ) );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldCallProcedureWithAllDefaultArgument()
    {
        //Given/When
        Result res = db.execute( "CALL org.neo4j.procedure.defaultValues" );

        // Then
        assertThat( res.next(), equalTo( map( "string", "a string", "integer", 42L, "aFloat", 3.14, "aBoolean", true ) ) );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldCallProcedureWithOneProvidedRestDefaultArgument()
    {
        //Given/When
        Result res = db.execute( "CALL org.neo4j.procedure.defaultValues('another string')" );

        // Then
        assertThat( res.next(), equalTo( map( "string", "another string", "integer", 42L, "aFloat", 3.14, "aBoolean", true ) ) );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldCallProcedureWithTwoProvidedRestDefaultArgument()
    {
        //Given/When
        Result res = db.execute( "CALL org.neo4j.procedure.defaultValues('another string', 1337)" );

        // Then
        assertThat( res.next(), equalTo( map( "string", "another string", "integer", 1337L, "aFloat", 3.14, "aBoolean", true ) ) );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldCallProcedureWithThreeProvidedRestDefaultArgument()
    {
        //Given/When
        Result res = db.execute( "CALL org.neo4j.procedure.defaultValues('another string', 1337, 2.718281828)" );

        // Then
        assertThat( res.next(), equalTo( map( "string", "another string", "integer", 1337L, "aFloat", 2.718281828, "aBoolean", true ) ) );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldCallProcedureWithFourProvidedRestDefaultArgument()
    {
        //Given/When
        Result res = db.execute( "CALL org.neo4j.procedure.defaultValues('another string', 1337, 2.718281828, false)" );

        // Then
        assertThat( res.next(), equalTo( map( "string", "another string", "integer", 1337L, "aFloat", 2.718281828, "aBoolean", false ) ) );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldGiveNiceErrorMessageOnWrongStaticType()
    {
        // When
        try ( Transaction ignore = db.beginTx() )
        {
            //Make sure argument here is not auto parameterized away as that will drop all type information on the floor
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.simpleArgument('42')" ) );
            assertThat( exception.getMessage(), startsWith( "Type mismatch: expected Integer but was String (line 1, column 41 (offset: 40))" ) );
        }
    }

    @Test
    void shouldGiveNiceErrorMessageWhenNoArguments()
    {
        //Expect
        // When
        try ( Transaction ignore = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.simpleArgument()" ) );
            assertThat( exception.getMessage(), containsStringIgnoreNewlines( String.format(
                    "Procedure call does not provide the required number of arguments: got 0 expected 1.%n%n" +
                            "Procedure org.neo4j.procedure.simpleArgument has signature: " +
                            "org.neo4j.procedure.simpleArgument(name :: INTEGER?) :: someVal :: INTEGER?%n" +
                            "meaning that it expects 1 argument of type INTEGER?" ) ) );
        }
    }

    @Test
    void shouldGiveNiceErrorWhenMissingArgumentsToVoidFunction()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.sideEffectWithDefault()" ) );
            assertThat( exception.getMessage(), containsStringIgnoreNewlines( String.format(
                    "Procedure call does not provide the required number of arguments: got 1 expected 3.%n%n" +
                            "Procedure org.neo4j.procedure.sideEffectWithDefault has signature: org.neo4j.procedure" +
                            ".sideEffectWithDefault(label :: STRING?, propertyKey :: STRING?, value  =  Zhang Wei :: STRING?) :: VOID%n" +
                            "meaning that it expects 3 arguments of type STRING?, STRING?, STRING? (line 1, column 1 (offset: 0))" ) ) );
        }
    }

    @Test
    void shouldShowDescriptionWhenMissingArguments()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.nodeWithDescription()" ) );
            assertThat( exception.getMessage(), containsStringIgnoreNewlines( String.format(
                    "Procedure call does not provide the required number of arguments: got 0 expected 1.%n%n" +
                            "Procedure org.neo4j.procedure.nodeWithDescription has signature: " +
                            "org.neo4j.procedure.nodeWithDescription(node :: NODE?) :: node :: NODE?%n" + "meaning that it expects 1 argument of type NODE?%n" +
                            "Description: This is a description (line 1, column 1 (offset: 0))" ) ) );
        }
    }

    @Test
    void shouldCallDelegatingProcedure()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.delegatingProcedure", map( "name", 43L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 43L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallRecursiveProcedure()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.recursiveSum", map( "order", 10L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 55L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithGenericArgument()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.genericArguments([ ['graphs'], ['are'], ['everywhere']], " + "[ [[1, 2, 3]], [[4, 5]]] )" );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 5L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithMapArgument()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.mapArgument({foo: 42, bar: 'hello'})" );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 2L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithMapArgumentDefaultingToNull()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.mapWithNullDefault()" );

            // Then
            assertThat( res.next(), equalTo( map( "map", null ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithMapArgumentDefaultingToMap()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.mapWithOtherDefault" );

            // Then
            assertThat( res.next(), equalTo( map( "map", map( "default", true ) ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithListWithDefault()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.listWithDefault" );

            // Then
            assertThat( res.next(), equalTo( map( "list", asList( 42L, 1337L ) ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithGenericListWithDefault()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.genericListWithDefault" );

            // Then
            assertThat( res.next(), equalTo( map( "list", asList( 42L, 1337L ) ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithByteArrayWithParameter() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.incrBytes($param)", map( "param", new byte[]{4, 5, 6} ) );

            // Then
            assertThat( res.columnAs( "bytes" ).next(), equalTo( new byte[]{5, 6, 7} ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithByteArrayWithParameterAndYield() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "WITH $param AS b CALL org.neo4j.procedure.incrBytes(b) YIELD bytes RETURN bytes", map( "param", new byte[]{7, 8, 9} ) );

            // Then
            assertThat( res.columnAs( "bytes" ).next(), equalTo( new byte[]{8, 9, 10} ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithByteArrayWithParameterAndYieldAndParameterReuse() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "WITH $param AS param CALL org.neo4j.procedure.incrBytes(param) YIELD bytes RETURN bytes, param",
                    map( "param", new byte[]{10, 11, 12} ) );

            // Then
            assertTrue( res.hasNext() );
            Map<String,Object> results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "bytes" ), equalTo( new byte[]{11, 12, 13} ) );
            assertThat( results.get( "param" ), equalTo( new byte[]{10, 11, 12} ) );
        }
    }

    @Test
    void shouldNotBeAbleCallWithCypherLiteralInByteArrayProcedure() throws Throwable
    {
        QueryExecutionException exception = assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction ignore = db.beginTx() )
            {
                Result result = db.execute( "CALL org.neo4j.procedure.incrBytes([1,2,3])" );
                result.next();
            }
        } );
        assertThat( exception.getMessage(), containsString( "Cannot convert 1 to byte for input to procedure" ) );
    }

    @Test
    void shouldCallProcedureListWithNull() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.genericListWithDefault(null)" );

            // Then
            assertThat( res.next(), equalTo( map( "list", null ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureListWithNullInList()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.genericListWithDefault([[42, null, 57]])" );

            // Then
            assertThat( res.next(), equalTo( map( "list", asList( 42L, null, 57L ) ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureWithNodeReturn()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            long nodeId = db.createNode().getId();

            // When
            Result res = db.execute( "CALL org.neo4j.procedure.node({id})", map( "id", nodeId ) );

            // Then
            Node node = (Node) res.next().get( "node" );
            assertThat( node.getId(), equalTo( nodeId ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallProcedureReturningNull()
    {
        Result res = db.execute( "CALL org.neo4j.procedure.node(-1)" );

        assertThat( res.next().get( "node" ), nullValue() );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldCallYieldProcedureReturningNull()
    {
        Result res = db.execute( "CALL org.neo4j.procedure.node(-1) YIELD node as node RETURN node" );

        assertThat( res.next().get( "node" ), nullValue() );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldGiveHelpfulErrorOnMissingProcedure()
    {
        QueryExecutionException exception = assertThrows( QueryExecutionException.class, () -> db.execute( "CALL someProcedureThatDoesNotExist" ) );
        assertThat( exception.getMessage(), equalTo( "There is no procedure with the name `someProcedureThatDoesNotExist` " +
                "registered for this database instance. Please ensure you've spelled the " +
                "procedure name correctly and that the procedure is properly deployed." ) );
    }

    @Test
    void shouldGiveHelpfulErrorOnExceptionMidStream()
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction ignore = db.beginTx() )
        {
            Result result = db.execute( "CALL org.neo4j.procedure.throwsExceptionInStream" );

            QueryExecutionException exception = assertThrows( QueryExecutionException.class, result::next );
            assertThat( exception.getMessage(),
                    equalTo( "Failed to invoke procedure `org.neo4j.procedure.throwsExceptionInStream`: Caused by: java.lang.RuntimeException: Kaboom" ) );
        }
    }

    @Test
    void shouldShowCauseOfError()
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction ignore = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.indexOutOfBounds" ) );
            assertThat( exception.getMessage(),
                    startsWith( "Failed to invoke procedure `org.neo4j.procedure.indexOutOfBounds`: Caused by: java.lang.ArrayIndexOutOfBoundsException" ) );
        }
    }

    @Test
    void shouldCallProcedureWithAccessToDB()
    {
        // When
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode( label( "Person" ) ).setProperty( "name", "Buddy Holly" );
        }

        // Then
        try ( Transaction ignore = db.beginTx() )
        {
            Result res = db.execute( "CALL org.neo4j.procedure.listCoolPeopleInDatabase" );

            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldLogLikeThereIsNoTomorrow()
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        db.shutdown();
        db = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider ).setUserLogProvider( logProvider ).newImpermanentDatabaseBuilder().setConfig(
                plugin_dir, plugins.directory().getAbsolutePath() ).setConfig( procedure_unrestricted, "org.neo4j.procedure.*" ).newGraphDatabase();

        // When
        try ( Transaction ignore = db.beginTx() )
        {
            Result res = db.execute( "CALL org.neo4j.procedure.logAround()" );
            while ( res.hasNext() )
            {
                res.next();
            }
        }

        // Then
        AssertableLogProvider.LogMatcherBuilder match = inLog( GlobalProcedures.class );
        logProvider.assertAtLeastOnce( match.debug( "1" ), match.info( "2" ), match.warn( "3" ), match.error( "4" ) );
    }

    @Test
    void shouldDenyReadOnlyProcedureToPerformWrites()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.readOnlyTryingToWrite()" ).next() );
            assertThat( exception.getMessage(), startsWith( "Write operations are not allowed" ) );
        }
    }

    @Test
    void shouldAllowWriteProcedureToPerformWrites()
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.writingProcedure()" ).close();
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, db.getAllNodes().stream().count() );
            tx.success();
        }
    }

    @Test
    void readProceduresShouldPresentThemSelvesAsReadQueries()
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result result = db.execute( "EXPLAIN CALL org.neo4j.procedure.integrationTestMe()" );
            assertEquals( result.getQueryExecutionType().queryType(), QueryExecutionType.QueryType.READ_ONLY );
            tx.success();
        }
    }

    @Test
    void readProceduresWithYieldShouldPresentThemSelvesAsReadQueries()
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result result = db.execute( "EXPLAIN CALL org.neo4j.procedure.integrationTestMe() YIELD someVal as v RETURN v" );
            assertEquals( result.getQueryExecutionType().queryType(), QueryExecutionType.QueryType.READ_ONLY );
            tx.success();
        }
    }

    @Test
    void writeProceduresShouldPresentThemSelvesAsWriteQueries()
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result result = db.execute( "EXPLAIN CALL org.neo4j.procedure.createNode('n')" );
            assertEquals( result.getQueryExecutionType().queryType(), QueryExecutionType.QueryType.READ_WRITE );
            tx.success();
        }
    }

    @Test
    void writeProceduresWithYieldShouldPresentThemSelvesAsWriteQueries()
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result result = db.execute( "EXPLAIN CALL org.neo4j.procedure.createNode('n') YIELD node as n RETURN n.prop" );
            assertEquals( result.getQueryExecutionType().queryType(), QueryExecutionType.QueryType.READ_WRITE );
            tx.success();
        }
    }

    @Test
    void shouldNotBeAbleToCallWriteProcedureThroughReadProcedure()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.readOnlyCallingWriteProcedure" ).next() );
            assertThat( exception.getMessage(), startsWith( "Write operations are not allowed" ) );
        }
    }

    @Test
    void shouldNotBeAbleToCallReadProcedureThroughWriteProcedureInWriteOnlyTransaction()
    {
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.writeOnly() ) )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.writeProcedureCallingReadProcedure" ).next() );
            assertThat( exception.getMessage(), startsWith( "Read operations are not allowed" ) );
        }
    }

    @Test
    void shouldBeAbleToCallWriteProcedureThroughWriteProcedure()
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.writeProcedureCallingWriteProcedure()" ).close();
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, db.getAllNodes().stream().count() );
            tx.success();
        }
    }

    @Test
    void shouldNotBeAbleToCallSchemaProcedureThroughWriteProcedureInWriteTransaction()
    {
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.write() ) )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.writeProcedureCallingSchemaProcedure" ).next() );
            assertThat( exception.getMessage(), startsWith( "Schema operations are not allowed" ) );
        }
    }

    @Test
    void shouldDenyReadOnlyProcedureToPerformSchema()
    {
        // Give
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.readOnlyTryingToWriteSchema" ).next() );
            assertThat( exception.getMessage(), startsWith( "Schema operations are not allowed" ) );
        }
    }

    @Test
    void shouldDenyReadWriteProcedureToPerformSchema()
    {
        // Give
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.readWriteTryingToWriteSchema" ).next() );
            assertThat( exception.getMessage(), equalTo( "Schema operations are not allowed for AUTH_DISABLED with FULL restricted to TOKEN_WRITE." ) );
        }
    }

    @Test
    void shouldAllowSchemaProcedureToPerformSchema()
    {
        // Give
        try ( Transaction tx = db.beginTx() )
        {
            // When
            db.execute( "CALL org.neo4j.procedure.schemaProcedure" );
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertTrue( db.schema().getConstraints().iterator().hasNext() );
            tx.success();
        }
    }

    @Test
    void shouldAllowSchemaCallReadOnly()
    {
        // Given
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = db.createNode().getId();
            tx.success();
        }

        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.schemaCallReadProcedure({id})", map( "id", nodeId ) );

            // Then
            Node node = (Node) res.next().get( "node" );
            assertThat( node.getId(), equalTo( nodeId ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldDenySchemaProcedureToPerformWrite()
    {
        // Give
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.schemaTryingToWrite" ).next() );
            assertThat( exception.getMessage(), containsString( "Cannot perform data updates in a transaction that has performed schema updates" ) );
        }
    }

    @Test
    void shouldCoerceLongToDoubleAtRuntimeWhenCallingProcedure()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.squareDouble", map( "value", 4L ) );

            // Then
            assertThat( res.next(), equalTo( map( "result", 16.0d ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCoerceListOfNumbersToDoublesAtRuntimeWhenCallingProcedure()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.avgNumberList({param})", map( "param", Arrays.<Number>asList( 1L, 2L, 3L ) ) );

            // Then
            assertThat( res.next(), equalTo( map( "result", 2.0d ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCoerceListOfMixedNumbers()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.avgDoubleList([{long}, {double}])", map( "long", 1L, "double", 2.0d ) );

            // Then
            assertThat( res.next(), equalTo( map( "result", 1.5d ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCoerceDoubleToLongAtRuntimeWhenCallingProcedure()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "CALL org.neo4j.procedure.squareLong", map( "value", 4L ) );

            // Then
            assertThat( res.next(), equalTo( map( "someVal", 16L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldBeAbleToCallVoidProcedure()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.sideEffect('PONTUS')" );

            assertThat( db.execute( "MATCH (n:PONTUS) RETURN count(n) AS c" ).next().get( "c" ), equalTo( 1L ) );
        }
    }

    @Test
    void shouldBeAbleToCallVoidProcedureWithDefaultValue()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.sideEffectWithDefault('Person','name')" );
            Result result = db.execute( "MATCH (n:Person) RETURN n.name AS name" );
            assertThat( result.next().get( "name" ), equalTo( "Zhang Wei" ) );
            assertFalse( result.hasNext() );
        }
    }

    @Test
    void shouldBeAbleToCallDelegatingVoidProcedure()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.delegatingSideEffect('SUTNOP')" );

            assertThat( db.execute( "MATCH (n:SUTNOP) RETURN count(n) AS c" ).next().get( "c" ), equalTo( 1L ) );
        }
    }

    @Test
    void shouldBeAbleToPerformWritesOnNodesReturnedFromReadOnlyProcedure()
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = db.createNode().getId();
            Node node = Iterators.single( db.execute( "CALL org.neo4j.procedure.node", map( "id", nodeId ) ).columnAs( "node" ) );
            node.setProperty( "name", "Stefan" );
            tx.success();
        }
    }

    @Test
    void shouldFailToShutdown()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.shutdown()" ) );
            assertThat( exception.getMessage(),
                    equalTo( "Failed to invoke procedure `org.neo4j.procedure.shutdown`: Caused by: java.lang.UnsupportedOperationException" ) );
        }
    }

    @Test
    void shouldBeAbleToWriteAfterCallingReadOnlyProcedure()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.execute( "CALL org.neo4j.procedure.simpleArgument(12)" ).close();
            db.createNode();
        }
    }

    @Test
    void shouldBeAbleToSpawnThreadsCreatingTransactionInProcedures() throws Throwable
    {
        // given
        Runnable doIt = () ->
        {
            Result result = db.execute( "CALL org.neo4j.procedure.supportedProcedure()" );
            while ( result.hasNext() )
            {
                result.next();
            }
            result.close();
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

        Result result = db.execute( "MATCH () RETURN count(*) as n" );
        assertThat( result.hasNext(), equalTo( true ) );
        while ( result.hasNext() )
        {
            assertThat( result.next().get( "n" ), equalTo( (long) numThreads ) );
        }
        result.close();
        assertThat( "Should be no exceptions in procedures", exceptionsInProcedure.isEmpty(), equalTo( true ) );
    }

    @Test
    void shouldBeAbleToUseCallYieldWithPeriodicCommit() throws IOException
    {
        // GIVEN
        String[] lines = IntStream.rangeClosed( 1, 100 )
                .boxed()
                .map( i -> Integer.toString( i ) )
                .toArray( String[]::new );
        String url = createCsvFile( lines);

        //WHEN
        Result result = db.execute( "USING PERIODIC COMMIT 1 " +
                                    "LOAD CSV FROM '" + url + "' AS line " +
                                    "CALL org.neo4j.procedure.createNode(line[0]) YIELD node as n " +
                                    "RETURN n.prop" );
        // THEN
        for ( int i = 1; i <= 100; i++ )
        {
            assertThat( result.next().get( "n.prop" ), equalTo( Integer.toString( i ) ) );
        }
        result.close();

        //Make sure all the lines has been properly commited to the database.
        String[] dbContents = db.execute( "MATCH (n) return n.prop" ).stream().map( m -> (String) m.get( "n.prop" ) ).toArray( String[]::new );
        assertThat( dbContents, equalTo( lines ) );
    }

    @Test
    void shouldFailIfUsingPeriodicCommitWithReadOnlyQuery() throws IOException
    {
        String url = createCsvFile( "13" );

        QueryExecutionException exception = assertThrows( QueryExecutionException.class,
                () -> db.execute( "USING PERIODIC COMMIT 1 " + "LOAD CSV FROM '" + url + "' AS line " +
                "CALL org.neo4j.procedure.simpleArgument(toInt(line[0])) YIELD someVal as val " + "RETURN val" ) );
        assertThat( exception.getMessage(), startsWith( "Cannot use periodic commit in a non-updating query (line 1, column 1 (offset: 0))" ) );
    }

    @Test
    void shouldBeAbleToUseCallYieldWithLoadCsvAndSet() throws IOException
    {
        // GIVEN
        String url = createCsvFile( "foo" );

        //WHEN
        Result result =
                db.execute( "LOAD CSV FROM '" + url + "' AS line CALL org.neo4j.procedure.createNode(line[0]) YIELD node as n SET n.p = 42 RETURN n.p" );
        // THEN
        assertThat( result.next().get( "n.p" ), equalTo( 42L ) );
    }

    @Test
    void shouldCallProcedureReturningPaths()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();
            Relationship rel = node1.createRelationshipTo( node2, RelationshipType.withName( "KNOWS" ) );

            // When
            Result res = db.execute( "CALL org.neo4j.procedure.nodePaths({node}) YIELD path RETURN path", map( "node", node1 ) );

            // Then
            assertTrue( res.hasNext() );
            Map<String,Object> value = res.next();
            Path path = (Path) value.get( "path" );
            assertThat( path.length(), equalTo( 1 ) );
            assertThat( path.startNode(), equalTo( node1 ) );
            assertThat( asList( path.relationships() ), equalTo( singletonList( rel ) ) );
            assertThat( path.endNode(), equalTo( node2 ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallStreamCloseWhenResultExhausted()
    {
        String query = "CALL org.neo4j.procedure.onCloseProcedure(0)";

        Result res = db.execute( query );

        assertTrue( res.hasNext() );
        res.next();

        assertFalse( onCloseCalled[0] );

        assertTrue( res.hasNext() );
        res.next();

        assertTrue( onCloseCalled[0] );
    }

    @Test
    void shouldCallStreamCloseWhenResultFiltered()
    {
        // This query should return zero rows
        String query = "CALL org.neo4j.procedure.onCloseProcedure(1) YIELD someVal WITH someVal WHERE someVal = 1337 RETURN someVal";

        Result res = db.execute( query );

        assertFalse( onCloseCalled[1] );

        assertFalse( res.hasNext() );

        assertTrue( onCloseCalled[1] );
    }

    private String createCsvFile( String... lines ) throws IOException
    {
        File file = plugins.createFile( "file" );

        try ( PrintWriter writer = FileUtils.newFilePrintWriter( file, StandardCharsets.UTF_8 ) )
        {
            for ( String line : lines )
            {
                writer.println( line );
            }
        }

        return file.toURI().toURL().toString();
    }

    @Test
    void shouldReturnNodeListTypedAsNodeList()
    {
        // When
        Result res = db.execute( "CALL org.neo4j.procedure.nodeList() YIELD nodes RETURN extract( x IN nodes | id(x) ) as ids" );

        // Then
        assertTrue( res.hasNext() );
        assertThat( ((List<?>) res.next().get( "ids" )).size(), equalTo( 2 ) );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldGiveNiceErrorMessageWhenAggregationFunctionInProcedureCall()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode( Label.label( "Person" ) );
            db.createNode( Label.label( "Person" ) );

            assertThrows( QueryExecutionException.class,
                    () -> db.execute( "MATCH (n:Person) CALL org.neo4j.procedure.nodeListArgument(collect(n)) YIELD someVal RETURN someVal" ) );
        }
    }

    @Test
    void shouldWorkWhenUsingWithToProjectList()
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode( Label.label( "Person" ) );
            db.createNode( Label.label( "Person" ) );

            // When
            Result res = db.execute(
                    "MATCH (n:Person) WITH collect(n) as persons " + "CALL org.neo4j.procedure.nodeListArgument(persons) YIELD someVal RETURN someVal" );

            // THEN
            assertThat( res.next().get( "someVal" ), equalTo( 2L ) );
        }
    }

    @Test
    void shouldNotAllowReadProcedureInNoneTransaction()
    {
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        AuthorizationViolationException exception =
                assertThrows( AuthorizationViolationException.class, () ->
                        {
                            try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.none() ) )
                            {
                                db.execute( "CALL org.neo4j.procedure.integrationTestMe()" );
                                tx.success();
                            }
                        } );
        assertThat( exception.getMessage(), startsWith( "Read operations are not allowed" ) );
    }

    @Test
    void shouldNotAllowWriteProcedureInReadOnlyTransaction()
    {
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        AuthorizationViolationException exception =
                assertThrows( AuthorizationViolationException.class, () ->
                        {
                            try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.read() ) )
                            {
                                db.execute( "CALL org.neo4j.procedure.writingProcedure()" );
                                tx.success();
                            }
                        } );
        assertThat( exception.getMessage(), startsWith( "Write operations are not allowed" ) );
    }

    @Test
    void shouldNotAllowSchemaWriteProcedureInWriteTransaction()
    {
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        AuthorizationViolationException exception =
                assertThrows( AuthorizationViolationException.class, () ->
                        {
                            try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.write() ) )
                            {
                                db.execute( "CALL org.neo4j.procedure.schemaProcedure()" );
                                tx.success();
                            }
                        } );
        assertThat( exception.getMessage(), startsWith( "Schema operations are not allowed" ) );
    }

    @Test
    void shouldCallProcedureWithDefaultNodeArgument()
    {
        //Given/When
        Result res = db.execute( "CALL org.neo4j.procedure.nodeWithDefault" );

        // Then
        assertThat( res.next(), equalTo( map( "node", null ) ) );
        assertFalse( res.hasNext() );
    }

    @Test
    void shouldIndicateDefaultValueWhenListingProcedures()
    {
        // Given/When
        List<Map<String,Object>> results = db.execute( "CALL dbms.procedures()" ).stream().filter(
                record -> record.get( "name" ).equals( "org.neo4j.procedure.nodeWithDefault" ) ).collect( Collectors.toList() );
        // Then
        assertFalse( results.isEmpty(), "Expected to find test procedure" );
        assertThat( results.get( 0 ).get( "signature" ), equalTo( "org.neo4j.procedure.nodeWithDefault(node = null :: NODE?) :: (node :: NODE?)" ) );
    }

    @Test
    void shouldShowDescriptionWhenListingProcedures()
    {
        // Given/When
        List<Map<String,Object>> results = db.execute( "CALL dbms.procedures()" ).stream().filter(
                record -> record.get( "name" ).equals( "org.neo4j.procedure.nodeWithDescription" ) ).collect( Collectors.toList() );
        // Then
        assertFalse( results.isEmpty(), "Expected to find test procedure" );
        assertThat( results.get( 0 ).get( "description" ), equalTo( "This is a description" ) );
    }

    @Test
    void shouldShowModeWhenListingProcedures()
    {
        // Given/When
        List<Map<String,Object>> results = db.execute( "CALL dbms.procedures()" ).stream().filter(
                record -> record.get( "name" ).equals( "org.neo4j.procedure.nodeWithDescription" ) ).collect( Collectors.toList() );
        // Then
        assertFalse( results.isEmpty(), "Expected to find test procedure" );
        assertThat( results.get( 0 ).get( "mode" ), equalTo( "WRITE" ) );
    }

    @Test
    void shouldIndicateDefaultValueWhenListingFunctions()
    {
        // Given/When
        List<Map<String,Object>> results = db.execute( "CALL dbms.functions()" ).stream().filter( record -> record.get( "name" )
                .equals( "org.neo4j.procedure.getNodeName" ) ).collect( Collectors.toList() );
        // Then
        assertFalse( results.isEmpty(), "Expected to find test function" );
        assertThat( results.get( 0 ).get( "signature" ), equalTo( "org.neo4j.procedure.getNodeName(node = null :: NODE?) :: (STRING?)" ) );
    }

    @Test
    void shouldShowDescriptionWhenListingFunctions()
    {
        // Given/When
        List<Map<String,Object>> results = db.execute( "CALL dbms.functions()" ).stream().filter(
                record -> record.get( "name" ).equals( "org.neo4j.procedure.functionWithDescription" ) ).collect( Collectors.toList() );
        // Then
        assertFalse( results.isEmpty(), "Expected to find test function" );
        assertThat( results.get( 0 ).get( "description" ), equalTo( "This is a description" ) );
    }

    @Test
    void shouldCallFunctionWithByteArrayWithParameter() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res = db.execute( "RETURN org.neo4j.procedure.decrBytes($param) AS bytes", map( "param", new byte[]{4, 5, 6} ) );

            // Then
            assertThat( res.columnAs( "bytes" ).next(), equalTo( new byte[]{3, 4, 5} ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldCallFuctionWithByteArrayWithBoundLiteral() throws Throwable
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            Result res =
                    db.execute( "WITH $param AS param RETURN org.neo4j.procedure.decrBytes(param) AS bytes, param", map( "param", new byte[]{10, 11, 12} ) );

            // Then
            assertTrue( res.hasNext() );
            Map<String,Object> results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "bytes" ), equalTo( new byte[]{9, 10, 11} ) );
            assertThat( results.get( "param" ), equalTo( new byte[]{10, 11, 12} ) );
        }
    }

    @Test
    void shouldNotAllowNonByteValuesInImplicitByteArrayConversionWithUserDefinedFunction()
    {
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction ignore = db.beginTx() )
                    {
                        //Make sure argument here is not auto parameterized away as that will drop all type information on the floor
                        Result result = db.execute( "RETURN org.neo4j.procedure.decrBytes([1,2,5]) AS bytes" );
                        result.next();
                    }
                } );
        assertThat( exception.getMessage(), containsString( "Cannot convert 1 to byte for input to procedure" ) );
    }

    @Test
    void shouldCallAggregationFunctionWithByteArrays()
    {
        // Given
        try ( Transaction ignore = db.beginTx() )
        {
            // When
            byte[][] data = new byte[3][];
            data[0] = new byte[]{1, 2, 3};
            data[1] = new byte[]{3, 2, 1};
            data[2] = new byte[]{1, 2, 1};
            Result res = db.execute( "UNWIND $data AS bytes RETURN org.neo4j.procedure.aggregateByteArrays(bytes) AS bytes", map( "data", data ) );

            // Then
            assertThat( res.columnAs( "bytes" ).next(), equalTo( new byte[]{5, 6, 5} ) );
            assertFalse( res.hasNext() );
        }
    }

    @Test
    void shouldUseGuardToDetectTransactionTermination()
    {
        QueryExecutionException exception = assertThrows( QueryExecutionException.class, () -> db.execute( "CALL org.neo4j.procedure.guardMe" ) );
        assertThat( exception.getMessage(), equalTo( "The transaction has been terminated. Retry your operation in a new " +
                "transaction, and you should see a successful result. Explicitly terminated by the user. " ) );
    }

    @Test
    void shouldMakeTransactionToFail()
    {
        //When
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode( Label.label( "Person" ) );
        }
        Result result = db.execute( "CALL org.neo4j.procedure.failingPersonCount" );
        assertThrows( TransactionFailureException.class, result::next );
    }

    public static class Output
    {
        public long someVal = 1337;

        public Output()
        {
        }

        public Output( long someVal )
        {
            this.someVal = someVal;
        }
    }

    public static class PrimitiveOutput
    {
        public String string;
        public long integer;
        public double aFloat;
        public boolean aBoolean;

        public PrimitiveOutput( String string, long integer, double aFloat, boolean aBoolean )
        {
            this.string = string;
            this.integer = integer;
            this.aFloat = aFloat;
            this.aBoolean = aBoolean;
        }
    }

    public static class MapOutput
    {
        public Map<String,Object> map;

        public MapOutput( Map<String,Object> map )
        {
            this.map = map;
        }
    }

    public static class ListOutput
    {
        public List<Long> list;

        public ListOutput( List<Long> list )
        {
            this.list = list;
        }
    }

    public static class BytesOutput
    {
        public byte[] bytes;

        public BytesOutput( byte[] bytes )
        {
            this.bytes = bytes;
        }
    }

    public static class DoubleOutput
    {
        public double result;

        public DoubleOutput()
        {
        }

        public DoubleOutput( double result )
        {
            this.result = result;
        }
    }

    public static class NodeOutput
    {
        public Node node;

        public NodeOutput()
        {

        }

        public NodeOutput( Node node )
        {
            this.node = node;
        }

        void setNode( Node node )
        {
            this.node = node;
        }
    }

    public static class MyOutputRecord
    {
        public String name;

        public MyOutputRecord( String name )
        {
            this.name = name;
        }
    }

    public static class PathOutputRecord
    {
        public Path path;

        public PathOutputRecord( Path path )
        {
            this.path = path;
        }
    }

    public static class NodeListRecord
    {
        public List<Node> nodes;

        public NodeListRecord( List<Node> nodes )
        {
            this.nodes = nodes;
        }
    }

    @SuppressWarnings( "unused" )
    public static class ClassWithProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Log log;

        @Context
        public TerminationGuard guard;

        @Context
        public ProcedureTransaction procedureTransaction;

        @Procedure
        public Stream<Output> guardMe()
        {
            procedureTransaction.terminate();
            guard.check();
            throw new IllegalStateException( "Should never have executed this!" );
        }

        @Procedure
        public Stream<Output> integrationTestMe()
        {
            return Stream.of( new Output() );
        }

        @Procedure
        public Stream<Output> failingPersonCount()
        {
            Result result = db.execute( "MATCH (n:Person) RETURN count(n) as count" );
            procedureTransaction.failure();
            return Stream.of( new Output( (Long) result.next().get( "count" ) ) );
        }

        @Procedure
        public Stream<Output> simpleArgument( @Name( "name" ) long someValue )
        {
            return Stream.of( new Output( someValue ) );
        }

        @Procedure
        public Stream<Output> simpleArgumentWithDefault( @Name( value = "name", defaultValue = "42" ) long someValue )
        {
            return Stream.of( new Output( someValue ) );
        }

        @Procedure
        public Stream<PrimitiveOutput> defaultValues( @Name( value = "string", defaultValue = "a string" ) String string,
                @Name( value = "integer", defaultValue = "42" ) long integer, @Name( value = "float", defaultValue = "3.14" ) double aFloat,
                @Name( value = "boolean", defaultValue = "true" ) boolean aBoolean )
        {
            return Stream.of( new PrimitiveOutput( string, integer, aFloat, aBoolean ) );
        }

        @Procedure
        public Stream<Output> nodeListArgument( @Name( "nodes" ) List<Node> nodes )
        {
            return Stream.of( new Output( nodes.size() ) );
        }

        @Procedure
        public Stream<Output> delegatingProcedure( @Name( "name" ) long someValue )
        {
            return db.execute( "CALL org.neo4j.procedure.simpleArgument", map( "name", someValue ) ).stream().map(
                    row -> new Output( (Long) row.get( "someVal" ) ) );
        }

        @Procedure
        public Stream<Output> recursiveSum( @Name( "order" ) long order )
        {
            if ( order == 0L )
            {
                return Stream.of( new Output( 0L ) );
            }
            else
            {
                Long prev = (Long) db.execute( "CALL org.neo4j.procedure.recursiveSum", map( "order", order - 1 ) ).next().get( "someVal" );
                return Stream.of( new Output( order + prev ) );
            }
        }

        @Procedure
        public Stream<Output> genericArguments( @Name( "stringList" ) List<List<String>> stringList, @Name( "longList" ) List<List<List<Long>>> longList )
        {
            return Stream.of( new Output( stringList.size() + longList.size() ) );
        }

        @Procedure
        public Stream<Output> mapArgument( @Name( "map" ) Map<String,Object> map )
        {
            return Stream.of( new Output( map.size() ) );
        }

        @Procedure
        public Stream<MapOutput> mapWithNullDefault( @Name( value = "map", defaultValue = "null" ) Map<String,Object> map )
        {
            return Stream.of( new MapOutput( map ) );
        }

        @Procedure
        public Stream<MapOutput> mapWithOtherDefault( @Name( value = "map", defaultValue = "{default: true}" ) Map<String,Object> map )
        {
            return Stream.of( new MapOutput( map ) );
        }

        @Procedure
        public Stream<ListOutput> listWithDefault( @Name( value = "list", defaultValue = "[42, 1337]" ) List<Long> list )
        {
            return Stream.of( new ListOutput( list ) );
        }

        @Procedure
        public Stream<ListOutput> genericListWithDefault( @Name( value = "list", defaultValue = "[[42, 1337]]" ) List<List<Long>> list )
        {
            return Stream.of( new ListOutput( list == null ? null : list.get( 0 ) ) );
        }

        @Procedure
        public Stream<BytesOutput> incrBytes( @Name( value = "bytes" ) byte[] bytes )
        {
            for ( int i = 0; i < bytes.length; i++ )
            {
                bytes[i] += 1;
            }
            return Stream.of( new BytesOutput( bytes ) );
        }

        @Procedure
        public Stream<NodeOutput> node( @Name( "id" ) long id )
        {
            NodeOutput nodeOutput = new NodeOutput();
            if ( id < 0 )
            {
                nodeOutput.setNode( null );
            }
            else
            {
                nodeOutput.setNode( db.getNodeById( id ) );
            }
            return Stream.of( nodeOutput );
        }

        @Procedure
        public Stream<DoubleOutput> squareDouble( @Name( "value" ) double value )
        {
            DoubleOutput output = new DoubleOutput( value * value );
            return Stream.of( output );
        }

        @Procedure
        public Stream<DoubleOutput> avgNumberList( @Name( "list" ) List<Number> list )
        {
            double sum = list.stream().reduce( ( l, r ) -> l.doubleValue() + r.doubleValue() ).orElse( 0.0d ).doubleValue();
            int count = list.size();
            DoubleOutput output = new DoubleOutput( sum / count );
            return Stream.of( output );
        }

        @Procedure
        public Stream<DoubleOutput> avgDoubleList( @Name( "list" ) List<Double> list )
        {
            double sum = list.stream().reduce( ( l, r ) -> l + r ).orElse( 0.0d );
            int count = list.size();
            DoubleOutput output = new DoubleOutput( sum / count );
            return Stream.of( output );
        }

        @Procedure
        public Stream<Output> squareLong( @Name( "value" ) long value )
        {
            Output output = new Output( value * value );
            return Stream.of( output );
        }

        @Procedure
        public Stream<Output> throwsExceptionInStream()
        {
            return Stream.generate( () ->
            {
                throw new RuntimeException( "Kaboom" );
            } );
        }

        @Procedure
        public Stream<Output> indexOutOfBounds()
        {
            int[] ints = {1, 2, 3};
            int foo = ints[4];
            return Stream.of( new Output() );
        }

        @Procedure
        public Stream<MyOutputRecord> listCoolPeopleInDatabase()
        {
            return db.findNodes( label( "Person" ) ).stream().map( n -> new MyOutputRecord( (String) n.getProperty( "name" ) ) );
        }

        @Procedure
        public Stream<Output> logAround()
        {
            log.debug( "1" );
            log.info( "2" );
            log.warn( "3" );
            log.error( "4" );
            return Stream.empty();
        }

        @Procedure
        public Stream<Output> readOnlyTryingToWrite()
        {
            db.createNode();
            return Stream.empty();
        }

        @Procedure( mode = WRITE )
        public Stream<Output> writingProcedure()
        {
            db.createNode();
            return Stream.empty();
        }

        @Procedure( mode = WRITE )
        public Stream<NodeOutput> createNode( @Name( "value" ) String value )
        {
            Node node = db.createNode();
            node.setProperty( "prop", value );
            NodeOutput out = new NodeOutput();
            out.setNode( node );
            return Stream.of( out );
        }

        @Procedure
        public Stream<Output> readOnlyCallingWriteProcedure()
        {
            return db.execute( "CALL org.neo4j.procedure.writingProcedure" ).stream().map( row -> new Output( 0 ) );
        }

        @Procedure( mode = WRITE )
        public Stream<Output> writeProcedureCallingWriteProcedure()
        {
            return db.execute( "CALL org.neo4j.procedure.writingProcedure" ).stream().map( row -> new Output( 0 ) );
        }

        @Procedure( mode = WRITE )
        public Stream<Output> writeProcedureCallingReadProcedure()
        {
            return db.execute( "CALL org.neo4j.procedure.integrationTestMe" ).stream().map( row -> new Output( 0 ) );
        }

        @Procedure( mode = WRITE )
        public Stream<Output> writeProcedureCallingSchemaProcedure()
        {
            return db.execute( "CALL org.neo4j.procedure.schemaProcedure" ).stream().map( row -> new Output( 0 ) );
        }

        @Procedure( mode = WRITE )
        public void sideEffect( @Name( "value" ) String value )
        {
            db.createNode( Label.label( value ) );
        }

        @Procedure( mode = WRITE )
        public void sideEffectWithDefault( @Name( "label" ) String label, @Name( "propertyKey" ) String propertyKey,
                /* Most common name, according to the internet */
                @Name( value = "value", defaultValue = "Zhang Wei" ) String value )
        {
            db.createNode( Label.label( label ) ).setProperty( propertyKey, value );
        }

        @Procedure
        public void shutdown()
        {
            db.shutdown();
        }

        @Procedure( mode = WRITE )
        public void delegatingSideEffect( @Name( "value" ) String value )
        {
            db.execute( "CALL org.neo4j.procedure.sideEffect", map( "value", value ) );
        }

        @Procedure( mode = WRITE )
        public void supportedProcedure() throws ExecutionException, InterruptedException
        {
            jobs.submit( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.createNode();
                    tx.success();
                }
                catch ( Exception e )
                {
                    exceptionsInProcedure.add( e );
                }
            } ).get();
        }

        @Procedure
        public Stream<PathOutputRecord> nodePaths( @Name( "node" ) Node node )
        {
            return db.execute( "WITH {node} AS node MATCH p=(node)-[*]->() RETURN p", map( "node", node ) ).stream().map(
                    record -> new PathOutputRecord( (Path) record.getOrDefault( "p", null ) ) );
        }

        @Procedure( mode = WRITE )
        public Stream<NodeOutput> nodeWithDefault( @Name( value = "node", defaultValue = "null" ) Node node )
        {
            return Stream.of( new NodeOutput( node ) );
        }

        @Description( "This is a description" )
        @Procedure( mode = WRITE )
        public Stream<NodeOutput> nodeWithDescription( @Name( "node" ) Node node )
        {
            return Stream.of( new NodeOutput( node ) );
        }

        @Procedure( mode = WRITE )
        public Stream<NodeListRecord> nodeList()
        {
            List<Node> nodesList = new ArrayList<>();
            nodesList.add( db.createNode() );
            nodesList.add( db.createNode() );

            return Stream.of( new NodeListRecord( nodesList ) );
        }

        @Procedure
        public void readOnlyTryingToWriteSchema()
        {
            db.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
        }

        @Procedure( mode = WRITE )
        public void readWriteTryingToWriteSchema()
        {
            db.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
        }

        @Procedure( mode = SCHEMA )
        public void schemaProcedure()
        {
            db.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
        }

        @Procedure( mode = SCHEMA )
        public Stream<NodeOutput> schemaCallReadProcedure( @Name( "id" ) long id )
        {
            return db.execute( "CALL org.neo4j.procedure.node(" + id + ")" ).stream().map( record ->
            {
                NodeOutput n = new NodeOutput();
                n.setNode( (Node) record.get( "node" ) );
                return n;
            } );
        }

        @Procedure( mode = SCHEMA )
        public void schemaTryingToWrite()
        {
            db.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
            db.createNode();
        }

        @Procedure( name = "org.neo4j.procedure.onCloseProcedure" )
        public Stream<Output> onCloseProcedure( @Name( "index" ) long index )
        {
            onCloseCalled[(int) index] = false;
            return Stream.of( 1L, 2L ).map( Output::new ).onClose( () -> onCloseCalled[(int) index] = true );
        }
    }

    public static class ClassWithFunctions
    {
        @UserFunction()
        public String getNodeName( @Name( value = "node", defaultValue = "null" ) Node node )
        {
            return "nodeName";
        }

        @Description( "This is a description" )
        @UserFunction()
        public long functionWithDescription()
        {
            return 0;
        }

        @UserFunction
        public byte[] decrBytes( @Name( value = "bytes" ) byte[] bytes )
        {
            for ( int i = 0; i < bytes.length; i++ )
            {
                bytes[i] -= 1;
            }
            return bytes;
        }

        @UserAggregationFunction
        public ByteArrayAggregator aggregateByteArrays()
        {
            return new ByteArrayAggregator();
        }

        public static class ByteArrayAggregator
        {
            byte[] aggregated;

            @UserAggregationUpdate
            public void update( @Name( "bytes" ) byte[] bytes )
            {
                if ( aggregated == null )
                {
                    aggregated = new byte[bytes.length];
                }
                for ( int i = 0; i < Math.min( bytes.length, aggregated.length ); i++ )
                {
                    aggregated[i] += bytes[i];
                }
            }

            @UserAggregationResult
            public byte[] result()
            {
                return aggregated == null ? new byte[0] : aggregated;
            }
        }
    }

    private static final ScheduledExecutorService jobs = Executors.newScheduledThreadPool( 5 );
}
