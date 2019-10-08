/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.builtinprocs;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.extension.Inject;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.single;

@ImpermanentEnterpriseDbmsExtension
class IndexProceduresIT
{
    @Inject
    GraphDatabaseService db;
    private static final String CREATE_INDEX_FORMAT = "db.createIndex";
    private static final String CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT = "db.createUniquePropertyConstraint";
    private static final String CREATE_NODE_KEY_CONSTRAINT_FORMAT = "db.createNodeKey";
    private static final Label label = Label.label( "Label" );
    private static final String prop = "prop";
    private static final String pattern = ":" + label + "(" + prop + ")";
    private static final String providerName = "native-btree-1.0";

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldCreateIndexWithName( String procedure )
    {
        shouldCreateIndexWithName( procedure, "MyIndex" );
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldCreateIndexWithNullName( String procedure )
    {
        shouldCreateIndexWithName( procedure, null );
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldNotCreateIndexWithEmptyName( String procedure )
    {
        final QueryExecutionException exception = assertThrows( QueryExecutionException.class, () ->
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        tx.execute( asProcedureCall( procedure, "" ) ).close();
                        tx.commit();
                    }
                }
        );
        final Throwable rootCause = getRootCause( exception );
        assertTrue( rootCause instanceof IllegalArgumentException );
        assertEquals( "Schema rule name cannot be the empty string or only contain whitespace.", rootCause.getMessage() );
    }

    private void shouldCreateIndexWithName( String procedure, String indexName )
    {
        // Given no indexes initially
        try ( Transaction tx = db.beginTx() )
        {
            final Iterator<IndexDefinition> indexes = tx.schema().getIndexes().iterator();
            assertFalse( indexes.hasNext() );
            tx.commit();
        }

        // When creating index / constraint with name
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( asProcedureCall( procedure, indexName ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();

        // Then we should be able to find the index by that name and no other indexes should exist.
        try ( Transaction tx = db.beginTx() )
        {
            final List<IndexDefinition> indexes = Iterables.asList( tx.schema().getIndexes() );
            assertEquals( 1, indexes.size() );
            final IndexDefinition index = indexes.get( 0 );
            if ( indexName != null && !indexName.isEmpty() )
            {
                assertEquals( "MyIndex", index.getName() );
            }
            assertEquals( label, single( index.getLabels() ) );
            assertEquals( prop, single( index.getPropertyKeys() ) );
            tx.commit();
        }
    }

    private void awaitIndexesOnline()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.commit();
        }
    }

    private static String asProcedureCall( String procedureName, String indexName )
    {
        if ( indexName == null )
        {
            return format( "CALL " + procedureName + "( NULL, \"%s\", \"%s\" )", pattern, providerName );
        }
        else
        {
            return format( "CALL " + procedureName + "( \"%s\", \"%s\", \"%s\" )", indexName, pattern, providerName );
        }
    }
}
