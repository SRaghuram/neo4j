/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;

import org.neo4j.collection.RawIterator;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.ProcedureITBase;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.StringValue;

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;

@EnterpriseDbmsExtension
class SystemBuiltInEnterpriseProceduresTest implements ProcedureITBase
{
    @Inject
    DatabaseManagementService databaseManagementService;

    public GraphDatabaseAPI getGraphDatabaseAPI()
    {
        return (GraphDatabaseAPI)databaseManagementService.database( SYSTEM_DATABASE_NAME );
    }

    @Test
    void listProcedures() throws Throwable
    {
        // When
        GraphDatabaseAPI system = getGraphDatabaseAPI();
        KernelTransaction transaction = system.beginTransaction( IMPLICIT, AnonymousContext.read() ).kernelTransaction();
        ProcedureHandle procedures = transaction.procedures().procedureGet( procedureName( "dbms", "procedures" ) );
        RawIterator<AnyValue[],ProcedureException> stream =
                transaction.procedures().procedureCallRead( procedures.id(), new AnyValue[0], ProcedureCallContext.EMPTY );

        // Then
        List<AnyValue[]> actual = asList( stream );
        List<Object[]> expected = getExpectedEnterpriseProcs();

        Map<String,AnyValue[]> resultMap = actual.stream().collect( toMap( row -> ((StringValue) row[1]).stringValue(), Function.identity() ) );
        // We cant use name (row[0]) for key because we have multiple ones with the same name (dbms.functuons and dbms.procedures())
        // thus we use the signature as key
        Map<String,Object[]> expectedMap = expected.stream().collect( toMap( row -> ((StringValue) row[1]).stringValue(), Function.identity() ) );
        assertThat( resultMap.keySet(), containsInAnyOrder( expectedMap.keySet().toArray() ) );
        for ( String procSignature : resultMap.keySet() )
        {
            AnyValue[] actualArray = resultMap.get( procSignature );
            Object[] expectedArray = expectedMap.get( procSignature );
            assertNotNull( expectedArray, "Got an unexpected entry for " + procSignature + " =>\n" + printElementsOfArray( actualArray ) );
            assertEquals( expectedArray.length, actualArray.length, "Count of columns for " + procSignature + " does not match" );

            for ( int i = 1; i < actualArray.length; i++ )
            {
                @SuppressWarnings( "rawtypes" )
                Matcher matcher = (expectedArray[i] instanceof Matcher) ? (Matcher) expectedArray[i] : equalTo( expectedArray[i] );
                //noinspection unchecked
                assertThat( "Column " + i + " for " + procSignature + " does not match", actualArray[i], matcher );
            }
        }
        transaction.commit();
    }

    private String printElementsOfArray( AnyValue[] array )
    {
        StringJoiner result = new StringJoiner( System.lineSeparator(), System.lineSeparator(), "" );
        for ( AnyValue anyValue : array )
        {
            result.add( anyValue.toString() );
        }
        return result.toString();
    }

    @Test
    void mustBeAbleToListAvailableAnalyzers()
    {
        GraphDatabaseService system = databaseManagementService.database( SYSTEM_DATABASE_NAME );
        // Verify that a couple of expected analyzers are available.
        try ( org.neo4j.graphdb.Transaction tx = system.beginTx() )
        {
            Set<String> analyzers = new HashSet<>();
            try ( ResourceIterator<String> iterator = tx.execute( "CALL db.index.fulltext.listAvailableAnalyzers()" ).columnAs( "analyzer" ) )
            {
                while ( iterator.hasNext() )
                {
                    analyzers.add( iterator.next() );
                }
            }
            assertThat( analyzers, hasItem( "english" ) );
            assertThat( analyzers, hasItem( "swedish" ) );
            assertThat( analyzers, hasItem( "standard" ) );
            tx.commit();
        }

        // Verify that all analyzers have a description.
        try ( org.neo4j.graphdb.Transaction tx = system.beginTx() )
        {
            try ( Result result = tx.execute( "CALL db.index.fulltext.listAvailableAnalyzers()" ) )
            {
                while ( result.hasNext() )
                {
                    Map<String,Object> row = result.next();
                    Object description = row.get( "description" );
                    if ( !row.containsKey( "description" ) || !(description instanceof String) || ((String) description).trim().isEmpty() )
                    {
                        fail( "Found no description for analyzer: " + row );
                    }
                }
            }
            tx.commit();
        }
    }

    @Test
    void checkEnterpriseProceduresThatAreNotAllowedOnSystem()
    {
        List<String> queries = List.of( "CALL db.createIndex('My index',['Person'], ['name'], 'lucene+native-3.0')",
                "CALL db.createLabel('Foo')",
                "CALL db.createNodeKey('My node key', ['Person'], ['age'], 'lucene+native-3.0')",  // enterprise only
                "CALL db.createProperty('bar')",
                "CALL db.createRelationshipType('BAZ')",
                "CALL db.createUniquePropertyConstraint('My unique property', ['Person'], ['id'], 'lucene+native-3.0')",
                "CALL db.index.fulltext.createNodeIndex('businessNameIndex', ['Business'],['name'])",
                "CALL db.index.fulltext.createRelationshipIndex('is owner of index', ['IS_OWNER_OF'],['name'])",
                "CALL tx.setMetaData( { User: 'Sascha' } )",
                "CALL db.index.fulltext.drop('businessNameIndex')" );

        // First validate that all queries can actually run on normal db
        final GraphDatabaseService db = databaseManagementService.database( DEFAULT_DATABASE_NAME );
        for ( String q : queries )
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                tx.execute( q ).close();
                tx.commit();
            }
        }

        // Then validate that they can't run in system
        GraphDatabaseService system = databaseManagementService.database( SYSTEM_DATABASE_NAME );
        for ( String q : queries )
        {
            try ( org.neo4j.graphdb.Transaction tx = system.beginTx() )
            {
                // When & Then
                RuntimeException exception = assertThrows( RuntimeException.class, () -> tx.execute( q ).close() );
                assertTrue( exception.getMessage()
                                .startsWith( "Not a recognised system command or procedure. This Cypher command can only be executed in a user database:" ),
                        "Wrong error message for '" + q + "' => " + exception.getMessage() );
            }
        }
    }
}
