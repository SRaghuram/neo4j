/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.jar.JarBuilder;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.plugin_dir;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

@TestDirectoryExtension
public class ProcedureIT
{
    @Inject
    private TestDirectory plugins;

    private static final ScheduledExecutorService jobs = Executors.newScheduledThreadPool( 5 );

    private static List<Exception> exceptionsInProcedure = Collections.synchronizedList( new ArrayList<>() );
    private GraphDatabaseService db;
    private GraphDatabaseService system;
    static boolean[] onCloseCalled;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() throws IOException
    {
        exceptionsInProcedure.clear();
        new JarBuilder().createJarFor( plugins.createFile( "myProcedures.jar" ), ClassWithProcedures.class );
        new JarBuilder().createJarFor( plugins.createFile( "myProceduresWithKernelTransaction.jar" ), ClassWithProceduresUsingKernelTransaction.class );
        new JarBuilder().createJarFor( plugins.createFile( "myFunctions.jar" ), ClassWithFunctions.class );
        new JarBuilder().createJarFor( plugins.createFile( "myWriteFunctions.jar" ), WriteInFunction.class );
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder()
                .impermanent()
                .setConfig( plugin_dir, plugins.absolutePath() )
                .setConfig( procedure_unrestricted, List.of("com.neo4j.procedure.startTimeOfKernelTransaction") )
                .build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
        system = managementService.database( SYSTEM_DATABASE_NAME );
        onCloseCalled = new boolean[2];
    }

    @AfterEach
    void tearDown()
    {
        if ( this.db != null )
        {
            this.managementService.shutdown();
        }
    }

    @AfterAll
    static void cleanUp()
    {
        jobs.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithParameterMap( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.simpleArgument", runtime ), map( "name", 42L ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 42L ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithDefaultArgument( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.simpleArgumentWithDefault", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 42L ) );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithInheritedResultType( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given/When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.inheritedOutput", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 42L, "anotherVal", "a" ) );
            assertThat( res.next() ).isEqualTo( map( "someVal", 42L, "anotherVal", "b" ) );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallYieldProcedureWithDefaultArgument( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given/When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.simpleArgumentWithDefault() YIELD someVal as n RETURN n + 1295 as val", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "val", 1337L ) );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithAllDefaultArgument( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.defaultValues", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "string", "a string", "integer", 42L, "aFloat", 3.14, "aBoolean", true ) );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithOneProvidedRestDefaultArgument( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.defaultValues('another string')", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "string", "another string", "integer", 42L, "aFloat", 3.14, "aBoolean", true ) );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithTwoProvidedRestDefaultArgument( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.defaultValues('another string', 1337)", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "string", "another string", "integer", 1337L, "aFloat", 3.14, "aBoolean", true ) );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithThreeProvidedRestDefaultArgument( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.defaultValues('another string', 1337, 2.718281828)", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "string", "another string", "integer", 1337L, "aFloat", 2.718281828, "aBoolean", true ) );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithFourProvidedRestDefaultArgument( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.defaultValues('another string', 1337, 2.718281828, false)", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "string", "another string", "integer", 1337L, "aFloat", 2.718281828, "aBoolean", false ) );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGiveNiceErrorMessageOnWrongStaticType( String runtime )
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            //Make sure argument here is not auto parameterized away as that will drop all type information on the floor
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.simpleArgument('42')", runtime ) ) );
            assertThat( exception.getMessage() ).startsWith( "Type mismatch: expected Integer but was String" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGiveNiceErrorMessageWhenNoArguments( String runtime )
    {
        //Expect
        // When
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows( QueryExecutionException.class, () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.simpleArgument()", runtime ) ) );
            assertThat( normalizeString( exception.getMessage() ) ).startsWith( format(
                    "Procedure call does not provide the required number of arguments: " +
                    "got 0 expected at least 1 (total: 1, 0 of which have default values).%n%n" +
                    "Procedure com.neo4j.procedure.simpleArgument has signature: " +
                    "com.neo4j.procedure.simpleArgument(name :: INTEGER?) :: someVal :: INTEGER?%n" +
                    "meaning that it expects at least 1 argument of type INTEGER?" ) );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGiveNiceErrorMessageWhenTooManyArguments( String runtime )
    {
        //Expect
        // When
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows(
                    QueryExecutionException.class,
                    () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.simpleArgument(1, 2)", runtime ) )
            );
            assertThat( normalizeString( exception.getMessage() ) ).startsWith(
                    format( "Procedure call provides too many arguments: got 2 expected no more than 1.%n%n" +
                                   "Procedure com.neo4j.procedure.simpleArgument has signature: " +
                                   "com.neo4j.procedure.simpleArgument(name :: INTEGER?) :: someVal :: INTEGER?%n" +
                                   "meaning that it expects at least 1 argument of type INTEGER?" ) );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGiveNiceErrorMessageWhenTooManyArgumentsAndNoneRequired( String runtime )
    {
        //Expect
        // When
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.integrationTestMe(1, 2)", runtime ) ) );
            assertThat( normalizeString( exception.getMessage() ) ).startsWith(
                    format( "Procedure call provides too many arguments: got 2 expected none.%n%n" +
                                   "Procedure com.neo4j.procedure.integrationTestMe has signature: " +
                                   "com.neo4j.procedure.integrationTestMe() :: someVal :: INTEGER?%n" +
                                   "meaning that it expects no arguments" ) );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGiveNiceErrorMessageWhenMissingArgumentWhenDefaultArguments( String runtime )
    {
        //Expect
        // When
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows(
                    QueryExecutionException.class,
                    () -> tx.execute( format( "CYPHER runtime=%s CALL db.awaitIndex()", runtime ) )
            );
            assertThat( normalizeString( exception.getMessage() ) ).startsWith( format(
                    "Procedure call does not provide the required number of arguments: " +
                    "got 0 expected at least 1 (total: 2, 1 of which have default values).%n%n" +
                    "Procedure db.awaitIndex has signature: " +
                    "db.awaitIndex(indexName :: STRING?, timeOutSeconds  =  300 :: INTEGER?) :: VOID%n" +
                    "meaning that it expects at least 1 argument of type STRING?" ) );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGiveNiceErrorWhenMissingArgumentsToVoidFunction( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.sideEffectWithDefault()", runtime ) ) );
            assertThat( normalizeString( exception.getMessage() ) ).startsWith( format(
                    "Procedure call does not provide the required number of arguments: " +
                    "got 0 expected at least 2 (total: 3, 1 of which have default values).%n%n" +
                    "Procedure com.neo4j.procedure.sideEffectWithDefault has signature: com.neo4j.procedure" +
                    ".sideEffectWithDefault(label :: STRING?, propertyKey :: STRING?, value  =  'Zhang Wei' :: STRING?) :: VOID%n" +
                    "meaning that it expects at least 2 arguments of types STRING?, STRING?%n " ) );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldShowDescriptionWhenMissingArguments( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute(
                            format("CYPHER runtime=%s CALL com.neo4j.procedure.nodeWithDescription()", runtime )
                    ) );
            assertThat( normalizeString( exception.getMessage() ) ).startsWith( format(
                    "Procedure call does not provide the required number of arguments: " +
                    "got 0 expected at least 1 (total: 1, 0 of which have default values).%n%n" +
                    "Procedure com.neo4j.procedure.nodeWithDescription has signature: " +
                    "com.neo4j.procedure.nodeWithDescription(node :: NODE?) :: node :: NODE?%n" +
                    "meaning that it expects at least 1 argument of type NODE?%n" +
                    "Description: This is a description" ) );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallDelegatingProcedure( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.delegatingProcedure", runtime ),  map( "name", 43L ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 43L ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallRecursiveProcedure( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.recursiveSum", runtime ),  map( "order", 10L ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 55L ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithGenericArgument( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.genericArguments([ ['graphs'], ['are'], ['everywhere']], " + "[ [[1, 2, 3]], [[4, 5]]] )", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 5L ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithMapArgument( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.mapArgument({foo: 42, bar: 'hello'})", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 2L ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldAcceptSubtypesInDefault( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.procedureWithSubtypeDefaults()", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "map",
                    map( "defaultMap", emptyMap(), "defaultList", emptyList(), "defaultBoolean", true, "defaultInteger", 42L, "defaultFloat", 3.14,
                            "defaultString", "foo", "defaultNumberInteger", 42L, "defaultNumberFloat", 3.14, "defaultNullObject", null, "defaultNullMap", null,
                            "defaultNullList", null ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithMapArgumentDefaultingToMap( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.mapWithOtherDefault", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "map", map( "default", true ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithListWithDefault( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.listWithDefault", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "list", asList( 42L, 1337L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithGenericListWithDefault( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.genericListWithDefault", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "list", asList( 42L, 1337L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithByteArrayWithParameter( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.incrBytes($param)", runtime ),  map( "param", new byte[]{4, 5, 6} ) );

            // Then
            assertThat( res.columnAs( "bytes" ).next() ).isEqualTo( new byte[]{5, 6, 7} );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithByteArrayWithParameterAndYield( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s WITH $param AS b CALL com.neo4j.procedure.incrBytes(b) YIELD bytes RETURN bytes", runtime ),  map( "param", new byte[]{7, 8, 9} ) );

            // Then
            assertThat( res.columnAs( "bytes" ).next() ).isEqualTo( new byte[]{8, 9, 10} );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithByteArrayWithParameterAndYieldAndParameterReuse( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s WITH $param AS param CALL com.neo4j.procedure.incrBytes(param) YIELD bytes RETURN bytes, param", runtime),
                    map( "param", new byte[]{10, 11, 12} ) );

            // Then
            assertTrue( res.hasNext() );
            Map<String,Object> results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "bytes" ) ).isEqualTo( new byte[]{11, 12, 13} );
            assertThat( results.get( "param" ) ).isEqualTo( new byte[]{10, 11, 12} );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotBeAbleCallWithCypherLiteralInByteArrayProcedure( String runtime )
    {
        QueryExecutionException exception = assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                Result result = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.incrBytes([1,2,3])", runtime ) );
                result.next();
            }
        } );
        assertThat( exception.getMessage() ).contains( "Cannot convert 1 to byte for input to procedure" );
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureListWithNull( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.genericListWithDefault(null)", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "list", null ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureListWithNullInList( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.genericListWithDefault([[42, null, 57]])", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "list", asList( 42L, null, 57L ) ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithNodeReturn( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = tx.createNode().getId();

            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.node($id)", runtime ),  map( "id", nodeId ) );

            // Then
            Node node = (Node) res.next().get( "node" );
            assertThat( node.getId() ).isEqualTo( nodeId );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureReturningNull( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.node(-1)", runtime ) );

            assertThat( res.next().get( "node" ) ).isNull();
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallYieldProcedureReturningNull( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.node(-1) YIELD node as node RETURN node", runtime ) );

            assertThat( res.next().get( "node" ) ).isNull();
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGiveHelpfulErrorOnMissingProcedure( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> transaction.execute( format( "CYPHER runtime=%s CALL someProcedureThatDoesNotExist", runtime ) ) );
            assertThat( exception.getMessage() ).isEqualTo( "There is no procedure with the name `someProcedureThatDoesNotExist` " +
                    "registered for this database instance. Please ensure you've spelled the " +
                    "procedure name correctly and that the procedure is properly deployed." );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGiveHelpfulErrorOnExceptionMidStream( String runtime )
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.throwsExceptionInStream", runtime ) );

            QueryExecutionException exception = assertThrows( QueryExecutionException.class, result::next );
            assertThat( exception.getMessage() ).isEqualTo(
                    "Failed to invoke procedure `com.neo4j.procedure.throwsExceptionInStream`: Caused by: java.lang.RuntimeException: Kaboom" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldShowCauseOfError( String runtime )
    {
        // Given
        // run in tx to avoid having to wait for tx rollback on shutdown
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows( QueryExecutionException.class,
                    () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.indexOutOfBounds", runtime ) ).next() );
            assertThat( exception.getMessage() ).startsWith(
                    "Failed to invoke procedure `com.neo4j.procedure.indexOutOfBounds`: Caused by: java.lang.ArrayIndexOutOfBoundsException" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithAccessToDB( String runtime )
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label( "Person" ) ).setProperty( "name", "Buddy Holly" );
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.listCoolPeopleInDatabase", runtime ) );

            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldLogLikeThereIsNoTomorrow( String runtime )
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        managementService.shutdown();
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder().setInternalLogProvider( logProvider ).setUserLogProvider(
                logProvider ).impermanent()
                .setConfig( plugin_dir, plugins.absolutePath() )
                .setConfig( procedure_unrestricted, List.of( "com.neo4j.procedure.*" ) ).build();
        db = managementService.database( DEFAULT_DATABASE_NAME );

        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.logAround()", runtime ) );
            while ( res.hasNext() )
            {
                res.next();
            }
        }

        // Then
        assertThat( logProvider ).forClass( GlobalProcedures.class )
                .forLevel( DEBUG ).containsMessages( "1" )
                .forLevel( INFO ).containsMessages( "2" )
                .forLevel( WARN ).containsMessages( "3" )
                .forLevel( ERROR ).containsMessages( "4" );
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldDenyReadOnlyProcedureToPerformWrites( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.readOnlyTryingToWrite()", runtime ) ).next() );
            assertThat( exception.getMessage() ).startsWith( "Create node with labels '' is not allowed" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldAllowWriteProcedureToPerformWrites( String runtime )
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.writingProcedure()", runtime ) ).close();
            tx.commit();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, tx.getAllNodes().stream().count() );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void readProceduresShouldPresentThemSelvesAsReadQueries( String runtime )
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "EXPLAIN CALL com.neo4j.procedure.integrationTestMe()" );
            assertEquals( READ_ONLY, result.getQueryExecutionType().queryType() );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void readProceduresWithYieldShouldPresentThemSelvesAsReadQueries( String runtime )
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "EXPLAIN CALL com.neo4j.procedure.integrationTestMe() YIELD someVal as v RETURN v" );
            assertEquals( READ_ONLY, result.getQueryExecutionType().queryType() );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void writeProceduresShouldPresentThemSelvesAsWriteQueries( String runtime )
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "EXPLAIN CALL com.neo4j.procedure.createNode('n')" );
            assertEquals( READ_WRITE, result.getQueryExecutionType().queryType() );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void writeProceduresWithYieldShouldPresentThemSelvesAsWriteQueries( String runtime )
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "EXPLAIN CALL com.neo4j.procedure.createNode('n') YIELD node as n RETURN n.prop" );
            assertEquals( READ_WRITE, result.getQueryExecutionType().queryType() );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotBeAbleToCallWriteProcedureThroughReadProcedure( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.readOnlyCallingWriteProcedure", runtime ) ).next() );
            assertThat( exception.getMessage() ).contains( "Create node with labels '' is not allowed" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotGetReadAccessCallingReadProcedureThroughWriteProcedureInWriteOnlyTransaction( String runtime )
    {
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.EXPLICIT, AnonymousContext.full() ) )
        {
            tx.execute( "CREATE ()" );
            tx.commit();
        }

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.EXPLICIT, AnonymousContext.writeOnly() ) )
        {
            Result result = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.writeProcedureCallingReadProcedure", runtime ) );
            assertFalse( result.hasNext() );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToCallWriteProcedureThroughWriteProcedure( String runtime )
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.writeProcedureCallingWriteProcedure()", runtime ) ).close();
            tx.commit();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, tx.getAllNodes().stream().count() );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotBeAbleToCallSchemaProcedureThroughWriteProcedureInWriteTransaction( String runtime )
    {
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.EXPLICIT, AnonymousContext.write() ) )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class,
                            () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.writeProcedureCallingSchemaProcedure", runtime ) ).next() );
            assertThat( exception.getMessage() ).contains( "Schema operations are not allowed" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldDenyReadOnlyProcedureToPerformSchema( String runtime )
    {
        // Give
        try ( Transaction tx = db.beginTx() )
        {
            // When
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.readOnlyTryingToWriteSchema", runtime ) ).next() );
            assertThat( exception.getMessage() ).contains( "Schema operations are not allowed" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldDenyReadWriteProcedureToPerformSchema( String runtime )
    {
        // Give
        try ( Transaction tx = db.beginTx() )
        {
            // When
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.readWriteTryingToWriteSchema", runtime ) ).next() );
            assertThat( exception.getMessage() ).startsWith( "Schema operations are not allowed" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldAllowSchemaProcedureToPerformSchema( String runtime )
    {
        // Give
        try ( Transaction tx = db.beginTx() )
        {
            // When
            tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.schemaProcedure", runtime ) );
            tx.commit();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertTrue( tx.schema().getConstraints().iterator().hasNext() );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldAllowSchemaCallReadOnly( String runtime )
    {
        // Given
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = tx.createNode().getId();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.schemaCallReadProcedure($id)", runtime ),  map( "id", nodeId ) );

            // Then
            Node node = (Node) res.next().get( "node" );
            assertThat( node.getId() ).isEqualTo( nodeId );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldDenySchemaProcedureToPerformWrite( String runtime )
    {
        // Give
        try ( Transaction tx = db.beginTx() )
        {
            // When
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.schemaTryingToWrite", runtime ) ).next() );
            assertThat( exception.getMessage() ).contains( "Cannot perform data updates in a transaction that has performed schema updates" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCoerceLongToDoubleAtRuntimeWhenCallingProcedure( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.squareDouble", runtime ),  map( "value", 4L ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "result", 16.0d ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCoerceListOfNumbersToDoublesAtRuntimeWhenCallingProcedure( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.avgNumberList($param)", runtime ),  map( "param", Arrays.<Number>asList( 1L, 2L, 3L ) ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "result", 2.0d ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCoerceListOfMixedNumbers( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.avgDoubleList([$long, $double])", runtime ),  map( "long", 1L, "double", 2.0d ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "result", 1.5d ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCoerceDoubleToLongAtRuntimeWhenCallingProcedure( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.squareLong", runtime ),  map( "value", 4L ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "someVal", 16L ) );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToCallVoidProcedure( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.sideEffect('PONTUS')", runtime ) );

            assertThat( tx.execute( "MATCH (n:PONTUS) RETURN count(n) AS c" ).next().get( "c" ) ).isEqualTo( 1L );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToCallVoidProcedureWithDefaultValue( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.sideEffectWithDefault('Person','name')", runtime ) );
            Result result = tx.execute( "MATCH (n:Person) RETURN n.name AS name" );
            assertThat( result.next().get( "name" ) ).isEqualTo( "Zhang Wei" );
            assertFalse( result.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToCallDelegatingVoidProcedure( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.delegatingSideEffect('SUTNOP')", runtime ) );

            assertThat( tx.execute( "MATCH (n:SUTNOP) RETURN count(n) AS c" ).next().get( "c" ) ).isEqualTo( 1L );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToPerformWritesOnNodesReturnedFromReadOnlyProcedure( String runtime )
    {
        // When
        try ( Transaction tx = db.beginTx() )
        {
            long nodeId = tx.createNode().getId();
            Node node = Iterators.single( tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.node", runtime ),  map( "id", nodeId ) ).columnAs( "node" ) );
            node.setProperty( "name", "Stefan" );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToWriteAfterCallingReadOnlyProcedure( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.simpleArgument(12)", runtime ) ).close();
            tx.createNode();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToSpawnThreadsCreatingTransactionInProcedures( String runtime ) throws Throwable
    {
        // given
        Runnable doIt = () ->
        {
            try ( Transaction transaction = db.beginTx() )
            {
                try ( Result result = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.supportedProcedure()", runtime ) ) )
                {
                    while ( result.hasNext() )
                    {
                        result.next();
                    }
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

        try ( Transaction transaction = db.beginTx() )
        {
            try ( Result result = transaction.execute( "MATCH () RETURN count(*) as n" ) )
            {
                assertThat( result.hasNext() ).isEqualTo( true );
                while ( result.hasNext() )
                {
                    assertThat( result.next().get( "n" ) ).isEqualTo( (long) numThreads );
                }
            }
            transaction.commit();
        }
        assertThat( exceptionsInProcedure.isEmpty() ).as( "Should be no exceptions in procedures" ).isEqualTo( true );
    }

    @Test
    void shouldBeAbleToUseCallYieldWithPeriodicCommit() throws Exception
    {
        // GIVEN
        String[] lines = IntStream.rangeClosed( 1, 100 )
                .boxed()
                .map( i -> Integer.toString( i ) )
                .toArray( String[]::new );
        String url = createCsvFile( lines);

        //WHEN
        int value = db.executeTransactionally(
                "USING PERIODIC COMMIT 1 " + "LOAD CSV FROM '" + url + "' AS line " + "CALL com.neo4j.procedure.createNode(line[0]) YIELD node as n " +
                        "RETURN n.prop", emptyMap(), result ->
                {
                    int counter = 1;
                    while ( result.hasNext() )
                    {
                        var row = result.next();
                        assertThat( row.get( "n.prop" ) ).isEqualTo( Integer.toString( counter++ ) );
                    }
                    return counter;
                } );
        assertEquals( 101, value );

        try ( Transaction transaction = db.beginTx() )
        {
            //Make sure all the lines has been properly commited to the database.
            String[] dbContents = transaction.execute( "MATCH (n) return n.prop" ).stream().map( m -> (String) m.get( "n.prop" ) ).toArray( String[]::new );
            assertThat( dbContents ).isEqualTo( lines );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldFailIfUsingPeriodicCommitWithReadOnlyQuery() throws IOException
    {
        String url = createCsvFile( "13" );

        try ( Transaction transaction = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows( QueryExecutionException.class, () -> transaction.execute(
                    "USING PERIODIC COMMIT 1 " + "LOAD CSV FROM '" + url + "' AS line " +
                            "CALL com.neo4j.procedure.simpleArgument(toInteger(line[0])) YIELD someVal as val " + "RETURN val" ) );
            assertThat( exception.getMessage() ).startsWith( "Cannot use periodic commit in a non-updating query" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToUseCallYieldWithLoadCsvAndSet() throws IOException
    {
        // GIVEN
        String url = createCsvFile( "foo" );

        try ( Transaction transaction = db.beginTx() )
        {
            //WHEN
            Result result = transaction.execute( "LOAD CSV FROM '" + url +
                    "' AS line CALL com.neo4j.procedure.createNode(line[0]) YIELD node as n SET n.p = 42 RETURN n.p" );
            // THEN
            assertThat( result.next().get( "n.p" ) ).isEqualTo( 42L );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureReturningPaths( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            Relationship rel = node1.createRelationshipTo( node2, RelationshipType.withName( "KNOWS" ) );

            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.nodePaths($node) YIELD path RETURN path", runtime ),  map( "node", node1 ) );

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

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallStreamCloseWhenResultExhausted( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            String query = "CALL com.neo4j.procedure.onCloseProcedure(0)";

            Result res = transaction.execute( query );

            assertFalse( onCloseCalled[0] );

            assertTrue( res.hasNext() );
            res.next();

            // Pipelined runtime will exhaust the stream on the first result because of batching, but Slotted/Interpreted will not

            assertTrue( res.hasNext() );
            res.next();

            assertTrue( onCloseCalled[0] );

            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallStreamCloseWhenResultFiltered( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // This query should return zero rows
            String query = format("CYPHER runtime=%s CALL com.neo4j.procedure.onCloseProcedure(1) YIELD someVal WITH someVal WHERE someVal = 1337 RETURN someVal", runtime );

            Result res = transaction.execute( query );

            assertFalse( onCloseCalled[1] );

            assertFalse( res.hasNext() );

            assertTrue( onCloseCalled[1] );
        }
    }

    private String createCsvFile( String... lines ) throws IOException
    {
        java.nio.file.Path file = plugins.createFile( "file" );

        try ( PrintWriter writer = FileUtils.newFilePrintWriter( file, StandardCharsets.UTF_8 ) )
        {
            for ( String line : lines )
            {
                writer.println( line );
            }
        }

        return file.toUri().toURL().toString();
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldReturnNodeListTypedAsNodeList( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.nodeList() YIELD nodes RETURN [ x IN nodes | id(x) ] as ids", runtime ) );

            // Then
            assertTrue( res.hasNext() );
            assertThat( ((List<?>) res.next().get( "ids" )).size() ).isEqualTo( 2 );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGiveNiceErrorMessageWhenAggregationFunctionInProcedureCall( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( "Person" ) );
            tx.createNode( Label.label( "Person" ) );

            assertThrows( QueryExecutionException.class,
                    () -> tx.execute( format( "CYPHER runtime=%s MATCH (n:Person) CALL com.neo4j.procedure.nodeListArgument(collect(n)) YIELD someVal RETURN someVal", runtime ) ) );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldWorkWhenUsingWithToProjectList( String runtime )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( "Person" ) );
            tx.createNode( Label.label( "Person" ) );

            // When
            Result res = tx.execute(
                    format( "CYPHER runtime=%s MATCH (n:Person) WITH collect(n) as persons " + "CALL com.neo4j.procedure.nodeListArgument(persons) YIELD someVal RETURN someVal", runtime ) );

            // THEN
            assertThat( res.next().get( "someVal" ) ).isEqualTo( 2L );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldGetNoResultsWithReadProcedureInAccessTransaction( String runtime )
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
            Result result = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.nodeIds()", runtime ) );

            // Then
            assertFalse( result.hasNext() );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotAllowWriteProcedureInReadOnlyTransaction( String runtime )
    {
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.EXPLICIT, AnonymousContext.read() ) )
                    {
                        tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.writingProcedure()", runtime ) );
                        tx.commit();
                    }
                } );
        assertThat( exception.getMessage() ).startsWith( "Create node with labels '' is not allowed" );
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotAllowSchemaWriteProcedureInWriteTransaction( String runtime )
    {
        GraphDatabaseAPI gdapi = (GraphDatabaseAPI) db;

        // When
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = gdapi.beginTransaction( KernelTransaction.Type.EXPLICIT, AnonymousContext.write() ) )
                    {
                        tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.schemaProcedure()", runtime ) );
                        tx.commit();
                    }
                } );
        assertThat( exception.getMessage() ).startsWith( "Schema operations are not allowed" );
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallProcedureWithDefaultNodeArgument( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //Given/When
            Result res = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.nodeWithDefault", runtime ) );

            // Then
            assertThat( res.next() ).isEqualTo( map( "node", null ) );
            assertFalse( res.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldIndicateDefaultValueWhenListingProcedures( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given/When
            List<Map<String,Object>> results = transaction.execute( format( "CYPHER runtime=%s CALL dbms.procedures()", runtime ) ).stream().filter(
                    record -> record.get( "name" ).equals( "com.neo4j.procedure.nodeWithDefault" ) ).collect( Collectors.toList() );
            // Then
            assertFalse( results.isEmpty(), "Expected to find test procedure" );
            assertThat( results.get( 0 ).get( "signature" ) ).isEqualTo( "com.neo4j.procedure.nodeWithDefault(node = null :: NODE?) :: (node :: NODE?)" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldShowDescriptionWhenListingProcedures( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given/When
            List<Map<String,Object>> results = transaction.execute( format( "CYPHER runtime=%s CALL dbms.procedures()", runtime ) ).stream().filter(
                    record -> record.get( "name" ).equals( "com.neo4j.procedure.nodeWithDescription" ) ).collect( Collectors.toList() );
            // Then
            assertFalse( results.isEmpty(), "Expected to find test procedure" );
            assertThat( results.get( 0 ).get( "description" ) ).isEqualTo( "This is a description" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldShowModeWhenListingProcedures( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given/When
            List<Map<String,Object>> results = transaction.execute( format( "CYPHER runtime=%s CALL dbms.procedures()", runtime ) ).stream().filter(
                    record -> record.get( "name" ).equals( "com.neo4j.procedure.nodeWithDescription" ) ).collect( Collectors.toList() );
            // Then
            assertFalse( results.isEmpty(), "Expected to find test procedure" );
            assertThat( results.get( 0 ).get( "mode" ) ).isEqualTo( "WRITE" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldIndicateDefaultValueWhenListingFunctions( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given/When
            List<Map<String,Object>> results = transaction.execute( format( "CYPHER runtime=%s CALL dbms.functions()", runtime ) ).stream()
                                .filter( record -> record.get( "name" ).equals( "com.neo4j.procedure.getNodeName" ) )
                                .collect( Collectors.toList() );
            // Then
            assertFalse( results.isEmpty(), "Expected to find test function" );
            assertThat( results.get( 0 ).get( "signature" ) ).isEqualTo( "com.neo4j.procedure.getNodeName(node = null :: NODE?) :: (STRING?)" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldShowDescriptionWhenListingFunctions( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given/When
            List<Map<String,Object>> results = transaction.execute( format( "CYPHER runtime=%s CALL dbms.functions()", runtime ) ).stream().filter(
                    record -> record.get( "name" ).equals( "com.neo4j.procedure.functionWithDescription" ) ).collect( Collectors.toList() );
            // Then
            assertFalse( results.isEmpty(), "Expected to find test function" );
            assertThat( results.get( 0 ).get( "description" ) ).isEqualTo( "This is a description" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallFunctionWithByteArrayWithParameter( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format("CYPHER runtime=%s RETURN com.neo4j.procedure.decrBytes($param) AS bytes", runtime ),  map( "param", new byte[]{4, 5, 6} ) );

            // Then
            assertThat( res.columnAs( "bytes" ).next() ).isEqualTo( new byte[]{3, 4, 5} );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallFunctionWithByteArrayWithBoundLiteral( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res =
                    tx.execute( format("CYPHER runtime=%s WITH $param AS param RETURN com.neo4j.procedure.decrBytes(param) AS bytes, param", runtime ),  map( "param", new byte[]{10, 11, 12} ) );

            // Then
            assertTrue( res.hasNext() );
            Map<String,Object> results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "bytes" ) ).isEqualTo( new byte[]{9, 10, 11} );
            assertThat( results.get( "param" ) ).isEqualTo( new byte[]{10, 11, 12} );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotAllowNonByteValuesInImplicitByteArrayConversionWithUserDefinedFunction( String runtime )
    {
        QueryExecutionException exception =
                assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        //Make sure argument here is not auto parameterized away as that will drop all type information on the floor
                        Result result = tx.execute( format("CYPHER runtime=%s RETURN com.neo4j.procedure.decrBytes([1,2,5]) AS bytes", runtime ) );
                        result.next();
                    }
                } );
        assertThat( exception.getMessage() ).contains( "Cannot convert 1 to byte for input to procedure" );
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldCallAggregationFunctionWithByteArrays( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            byte[][] data = new byte[3][];
            data[0] = new byte[]{1, 2, 3};
            data[1] = new byte[]{3, 2, 1};
            data[2] = new byte[]{1, 2, 1};
            Result res = tx.execute( format("CYPHER runtime=%s UNWIND $data AS bytes RETURN com.neo4j.procedure.aggregateByteArrays(bytes) AS bytes", runtime ),  map( "data", data ) );

            // Then
            assertThat( res.columnAs( "bytes" ).next() ).isEqualTo( new byte[]{5, 6, 5} );
            assertFalse( res.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldUseGuardToDetectTransactionTermination( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            QueryExecutionException exception = assertThrows( QueryExecutionException.class,
                    () -> transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.guardMe", runtime ) ).next() );
            assertThat( exception.getMessage() ).isEqualTo( "The transaction has been terminated. Retry your operation in a new " +
                    "transaction, and you should see a successful result. Explicitly terminated by the user. " );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldMakeTransactionToFail( String runtime )
    {
        //When
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( Label.label( "Person" ) );
        }
        try ( Transaction transaction = db.beginTx() )
        {
            Result result = transaction.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.failingPersonCount", runtime ) );
            assertThrows( QueryExecutionException.class, result::next );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToChangeBehaviourBasedOnProcedureCallContext( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.outputDependsOnYield()", runtime ) );

            // Then
            assertTrue(res.hasNext());
            Map<String,Object> results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "string" ) ).isEqualTo( "Yay" );
            assertThat( results.get( "integer" ) ).isEqualTo( 1L );
            assertThat( results.get( "aFloat" ) ).isEqualTo( 1.0 );
            assertThat( results.get( "aBoolean" ) ).isEqualTo( true );

            // When
            res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.outputDependsOnYield() yield string, integer, aFloat, aBoolean RETURN *", runtime ) );

            // Then
            assertTrue(res.hasNext());
            results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "string" ) ).isEqualTo( "Yay" );
            assertThat( results.get( "integer" ) ).isEqualTo( 1L );
            assertThat( results.get( "aFloat" ) ).isEqualTo( 1.0 );
            assertThat( results.get( "aBoolean" ) ).isEqualTo( true );

            // Not request "string" now should change result of other values:
            // When
            res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.outputDependsOnYield() yield integer, aFloat, aBoolean RETURN *", runtime ) );

            // Then
            assertTrue(res.hasNext());
            results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "integer" ) ).isEqualTo( 0L );
            assertThat( results.get( "aFloat" ) ).isEqualTo( 0.0 );
            assertThat( results.get( "aBoolean" ) ).isEqualTo( false );

            // Renaming should not interfere with this
            // When
            res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.outputDependsOnYield() yield string as s, integer as i, aFloat as f, aBoolean as b RETURN *", runtime ) );

            // Then
            assertTrue(res.hasNext());
            results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "s" ) ).isEqualTo( "Yay" );
            assertThat( results.get( "i" ) ).isEqualTo( 1L );
            assertThat( results.get( "f" ) ).isEqualTo( 1.0 );
            assertThat( results.get( "b" ) ).isEqualTo( true );

            // When
            res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.outputDependsOnYield() yield integer as i, aFloat as f, aBoolean as b RETURN *", runtime ) );

            // Then
            assertTrue(res.hasNext());
            results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "i" ) ).isEqualTo( 0L );
            assertThat( results.get( "f" ) ).isEqualTo( 0.0 );
            assertThat( results.get( "b" ) ).isEqualTo( false );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToChangeBehaviourBasedOnProcedureCallContextDatabase( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.outputDependsOnDatabase()", runtime ) );

            // Then
            assertTrue(res.hasNext());
            Map<String,Object> results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "string" ) ).isEqualTo( DEFAULT_DATABASE_NAME );
            assertThat( results.get( "aBoolean" ) ).isEqualTo( false );
        }

        // Given
        try ( Transaction tx = system.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.outputDependsOnDatabase()", runtime ) );

            // Then
            assertTrue(res.hasNext());
            Map<String,Object> results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "string" ) ).isEqualTo( SYSTEM_DATABASE_NAME );
            assertThat( results.get( "aBoolean" ) ).isEqualTo( true );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldBeAbleToGetKernelTransactionFromContext( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            // When
            Result res = tx.execute( format( "CYPHER runtime=%s CALL com.neo4j.procedure.startTimeOfKernelTransaction()", runtime ) );

            // Then
            assertTrue(res.hasNext());
            Map<String,Object> results = res.next();
            assertFalse( res.hasNext() );
            assertThat( results.get( "someVal" ).getClass() ).isEqualTo( Long.class );
        }
    }

    @Test
    void shouldNotUsePipelinedRuntimeForWritingProcedure()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "CALL com.neo4j.procedure.writingProcedure()" );
            String runtime = (String) result.getExecutionPlanDescription().getArguments().get( "runtime" );
            assertThat( runtime.toUpperCase() ).isNotEqualTo( "PIPELINED" );
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotWriteInFunction( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE (f:Foo {key: 'initialValue'})" );
            tx.commit();
        }

        assertThatThrownBy( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                // When
                tx.execute( format("CYPHER runtime=%s MATCH (f:Foo) RETURN com.neo4j.procedure.setProp(f, 'key', 'val')", runtime ) ).accept( row -> true );
                tx.commit();
            }
        } ).hasMessageStartingWith( "Set property for property 'key' is not allowed" );

        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "MATCH (f:Foo) RETURN f.key" );
            assertThat( result.hasNext() ).isTrue();
            assertThat( result.next().get( "f.key" ) ).isEqualTo( "initialValue" );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotWriteInUpdateInAggregationFunction( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE (f:Foo {key: 'initialValue'})" );
            tx.commit();
        }

        assertThatThrownBy( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                // When
                tx.execute( format("CYPHER runtime=%s MATCH (f:Foo) RETURN com.neo4j.procedure.writeInUpdate(f)", runtime ) ).accept( row -> true );
                tx.commit();
            }
        } ).hasMessageStartingWith( "Set property for property 'key' is not allowed" );

        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "MATCH (f:Foo) RETURN f.key" );
            assertThat( result.hasNext() ).isTrue();
            assertThat( result.next().get( "f.key" ) ).isEqualTo( "initialValue" );
            tx.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"INTERPRETED", "SLOTTED", "PIPELINED"})
    void shouldNotWriteInResultInAggregationFunction( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE (f:Foo {key: 'initialValue'})" );
            tx.commit();
        }

        assertThatThrownBy( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                // When
                tx.execute( format("CYPHER runtime=%s MATCH (f:Foo) RETURN com.neo4j.procedure.writeInResult(f)", runtime ) ).accept( row -> true );
                tx.commit();
            }
        } ).hasMessageStartingWith( "Set property for property 'key' is not allowed" );

        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "MATCH (f:Foo) RETURN f.key" );
            assertThat( result.hasNext() ).isTrue();
            assertThat( result.next().get( "f.key" ) ).isEqualTo( "initialValue" );
            tx.commit();
        }
    }

    @SuppressWarnings( "WeakerAccess" )
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

    @SuppressWarnings( "WeakerAccess" )
    public static class InheritedOutput extends Output
    {
        public String anotherVal;

        public InheritedOutput( final String anotherVal )
        {
            super( 42 );
            this.anotherVal = anotherVal;
        }
    }

    @SuppressWarnings( "WeakerAccess" )
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

    @SuppressWarnings( "WeakerAccess" )
    public static class MapOutput
    {
        public Map<String,Object> map;

        public MapOutput( Map<String,Object> map )
        {
            this.map = map;
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class ListOutput
    {
        public List<Long> list;

        public ListOutput( List<Long> list )
        {
            this.list = list;
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class BytesOutput
    {
        public byte[] bytes;

        public BytesOutput( byte[] bytes )
        {
            this.bytes = bytes;
        }
    }

    @SuppressWarnings( {"WeakerAccess"} )
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

    @SuppressWarnings( "WeakerAccess" )
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

    @SuppressWarnings( "WeakerAccess" )
    public static class MyOutputRecord
    {
        public String name;

        public MyOutputRecord( String name )
        {
            this.name = name;
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class PathOutputRecord
    {
        public Path path;

        public PathOutputRecord( Path path )
        {
            this.path = path;
        }
    }

    @SuppressWarnings( "WeakerAccess" )
    public static class NodeListRecord
    {
        public List<Node> nodes;

        public NodeListRecord( List<Node> nodes )
        {
            this.nodes = nodes;
        }
    }

    @SuppressWarnings( {"WeakerAccess"} )
    public static class ClassWithProceduresUsingKernelTransaction
    {
        @Context
        public KernelTransaction ktx;

        @Procedure
        public Stream<Output> startTimeOfKernelTransaction()
        {
            return Stream.of( new Output( ktx.startTime() ) );
        }
    }

    @SuppressWarnings( {"WeakerAccess"} )
    public static class ClassWithProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Log log;

        @Context
        public TerminationGuard guard;

        @Context
        public Transaction transaction;

        @Context
        public ProcedureCallContext callContext;

        @Procedure
        public Stream<Output> guardMe()
        {
            transaction.terminate();
            guard.check();
            throw new IllegalStateException( "Should never have executed this!" );
        }

        @Procedure
        public Stream<Output> integrationTestMe()
        {
            return Stream.of( new Output() );
        }

        @Procedure
        public Stream<Output> nodeIds()
        {
            Result result = transaction.execute( "MATCH (n) RETURN id(n) as nodeId" );
            var nodeIds = new ArrayList<Output>();
            while ( result.hasNext() )
            {
                nodeIds.add( new Output( (Long) result.next().get( "nodeId" ) ) );
            }
            return nodeIds.stream();
        }

        @Procedure
        public Stream<Output> failingPersonCount()
        {
            Result result = transaction.execute( "MATCH (n:Person) RETURN count(n) as count" );
            transaction.rollback();
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
        public Stream<InheritedOutput> inheritedOutput()
        {
            return Stream.of( "a", "b" ).map( InheritedOutput::new );
        }

        @Procedure
        public Stream<PrimitiveOutput> defaultValues( @Name( value = "string", defaultValue = "a string" ) String string,
                @Name( value = "integer", defaultValue = "42" ) long integer, @Name( value = "float", defaultValue = "3.14" ) double aFloat,
                @Name( value = "boolean", defaultValue = "true" ) boolean aBoolean )
        {
            return Stream.of( new PrimitiveOutput( string, integer, aFloat, aBoolean ) );
        }

        @Procedure
        public Stream<PrimitiveOutput> outputDependsOnYield()
        {
            boolean requestedString = callContext.outputFields().anyMatch( name -> name.equals( "string" ) );
            if ( requestedString )
            {
                return Stream.of( new PrimitiveOutput( "Yay", 1, 1.0, true ) );
            }
            return Stream.of( new PrimitiveOutput( "Ney", 0, 0.0, false ) );
        }

        @SystemProcedure
        @Procedure
        public Stream<PrimitiveOutput> outputDependsOnDatabase()
        {
            return Stream.of( new PrimitiveOutput( callContext.databaseName(), 0, 0.0, callContext.isSystemDatabase() ) );
        }

        @Procedure
        public Stream<Output> nodeListArgument( @Name( "nodes" ) List<Node> nodes )
        {
            return Stream.of( new Output( nodes.size() ) );
        }

        @Procedure
        public Stream<Output> delegatingProcedure( @Name( "name" ) long someValue )
        {
            return transaction
                    .execute( "CALL com.neo4j.procedure.simpleArgument", map( "name", someValue ) ).stream().map(
                    row -> new Output( (Long) row.get( "someVal" ) ) );
        }

        @Procedure
        public Stream<Output> recursiveSum( @Name( "order" ) long order )
        {
            if ( order == 0L )
            {
                return Stream.of( new Output( 0L ) );
            }
            Long prev = (Long) transaction
                    .execute( "CALL com.neo4j.procedure.recursiveSum", map( "order", order - 1 ) ).next().get( "someVal" );
            return Stream.of( new Output( order + prev ) );
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
        public Stream<MapOutput> procedureWithSubtypeDefaults(
                @Name( value = "a", defaultValue = "{}" ) Object defaultMap,
                @Name( value = "b", defaultValue = "[]" ) Object defaultList,
                @Name( value = "c", defaultValue = "true" ) Object defaultBoolean,
                @Name( value = "d", defaultValue = "42" ) Object defaultInteger,
                @Name( value = "e", defaultValue = "3.14" ) Object defaultFloat,
                @Name( value = "f", defaultValue = "foo" ) Object defaultString,
                @Name( value = "g", defaultValue = "42" ) Number defaultNumberInteger,
                @Name( value = "h", defaultValue = "3.14" ) Number defaultNumberFloat,
                @Name( value = "i", defaultValue = "null" ) Object defaultNullObject,
                @Name( value = "j", defaultValue = "null" ) Map<String,Object> defaultNullMap,
                @Name( value = "l", defaultValue = "null" ) List<Object> defaultNullList )
        {
            return Stream.of( new MapOutput( map(
                    "defaultMap", defaultMap,
                    "defaultList", defaultList,
                    "defaultBoolean", defaultBoolean,
                    "defaultInteger", defaultInteger,
                    "defaultFloat", defaultFloat,
                    "defaultString", defaultString,
                    "defaultNumberInteger", defaultNumberInteger,
                    "defaultNumberFloat", defaultNumberFloat,
                    "defaultNullObject", defaultNullObject,
                    "defaultNullMap", defaultNullMap,
                    "defaultNullList", defaultNullList
            ) ) );
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
                nodeOutput.setNode( transaction.getNodeById( id ) );
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
            double sum = list.stream().reduce( Double::sum ).orElse( 0.0d );
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
            return transaction.findNodes( label( "Person" ) ).stream().map( n -> new MyOutputRecord( (String) n.getProperty( "name" ) ) );
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
            transaction.createNode();
            return Stream.empty();
        }

        @Procedure( mode = WRITE )
        public Stream<Output> writingProcedure()
        {
            transaction.createNode();
            return Stream.empty();
        }

        @Procedure( mode = WRITE )
        public Stream<NodeOutput> createNode( @Name( "value" ) String value )
        {
            Node node = transaction.createNode();
            node.setProperty( "prop", value );
            NodeOutput out = new NodeOutput();
            out.setNode( node );
            return Stream.of( out );
        }

        @Procedure
        public Stream<Output> readOnlyCallingWriteProcedure()
        {
            return transaction.execute( "CALL com.neo4j.procedure.writingProcedure" ).stream().map(
                    row -> new Output( 0 ) );
        }

        @Procedure( mode = WRITE )
        public Stream<Output> writeProcedureCallingWriteProcedure()
        {
            return transaction.execute( "CALL com.neo4j.procedure.writingProcedure" ).stream().map(
                    row -> new Output( 0 ) );
        }

        @Procedure( mode = WRITE )
        public Stream<Output> writeProcedureCallingReadProcedure()
        {
            return transaction.execute( "CALL com.neo4j.procedure.nodeIds" ).stream().map(
                    row -> new Output( 0 ) );
        }

        @Procedure( mode = WRITE )
        public Stream<Output> writeProcedureCallingSchemaProcedure()
        {
            return transaction.execute( "CALL com.neo4j.procedure.schemaProcedure" ).stream().map(
                    row -> new Output( 0 ) );
        }

        @Procedure( mode = WRITE )
        public void sideEffect( @Name( "value" ) String value )
        {
            transaction.createNode( Label.label( value ) );
        }

        @Procedure( mode = WRITE )
        public void sideEffectWithDefault( @Name( "label" ) String label, @Name( "propertyKey" ) String propertyKey,
                /* Most common name, according to the internet */
                @Name( value = "value", defaultValue = "Zhang Wei" ) String value )
        {
            transaction.createNode( Label.label( label ) ).setProperty( propertyKey, value );
        }

        @Procedure( mode = WRITE )
        public void delegatingSideEffect( @Name( "value" ) String value )
        {
            transaction.execute( "CALL com.neo4j.procedure.sideEffect", map( "value", value ) );
        }

        @Procedure( mode = WRITE )
        public void supportedProcedure() throws ExecutionException, InterruptedException
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
                    exceptionsInProcedure.add( e );
                }
            } ).get();
        }

        @Procedure
        public Stream<PathOutputRecord> nodePaths( @Name( "node" ) Node node )
        {
            return transaction
                    .execute( "WITH $node AS node MATCH p=(node)-[*]->() RETURN p", map( "node", node ) ).stream().map(
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
            nodesList.add( transaction.createNode() );
            nodesList.add( transaction.createNode() );

            return Stream.of( new NodeListRecord( nodesList ) );
        }

        @Procedure
        public void readOnlyTryingToWriteSchema()
        {
            transaction.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
        }

        @Procedure( mode = WRITE )
        public void readWriteTryingToWriteSchema()
        {
            transaction.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
        }

        @Procedure( mode = SCHEMA )
        public void schemaProcedure()
        {
            transaction.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
        }

        @Procedure( mode = SCHEMA )
        public Stream<NodeOutput> schemaCallReadProcedure( @Name( "id" ) long id )
        {
            return transaction.execute( "CALL com.neo4j.procedure.node(" + id + ")" ).stream().map( record ->
            {
                NodeOutput n = new NodeOutput();
                n.setNode( (Node) record.get( "node" ) );
                return n;
            } );
        }

        @Procedure( mode = SCHEMA )
        public void schemaTryingToWrite()
        {
            transaction.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE" );
            transaction.createNode();
        }

        @Procedure( name = "com.neo4j.procedure.onCloseProcedure" )
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

    public static class WriteInFunction
    {
        @Context
        public Transaction transaction;

        @UserFunction
        public String setProp( @Name( "node" ) Node node, @Name( "name" ) String name, @Name( "value" ) String value )
        {
            node.setProperty( name, value );
            return value;
        }

        @UserAggregationFunction
        public WriteInUpdateAggregation writeInUpdate()
        {
            return new WriteInUpdateAggregation();
        }

        public static class WriteInUpdateAggregation
        {
            private long count;

            @UserAggregationUpdate
            public void aggregate( @Name( "node" ) Node node )
            {
                node.setProperty( "key", "val" );
                count++;
            }

            @UserAggregationResult
            public Long result()
            {
                return count;
            }
        }

        @UserAggregationFunction
        public WriteInResultAggregation writeInResult()
        {
            return new WriteInResultAggregation( transaction );
        }

        public static class WriteInResultAggregation
        {

            private Transaction transaction;
            private long count;

            WriteInResultAggregation( Transaction transaction )
            {
                this.transaction = transaction;
            }

            @UserAggregationUpdate
            public void aggregate( @Name( "node" ) Node node )
            {
                count++;
            }

            @UserAggregationResult
            public Long result()
            {
                transaction.execute( "MATCH (f:Foo) SET f.key = 'val'" );
                return count;
            }
        }
    }

    private static String normalizeString( String value )
    {
        return value.replaceAll( "\\r?\\n", System.lineSeparator() );
    }
}
