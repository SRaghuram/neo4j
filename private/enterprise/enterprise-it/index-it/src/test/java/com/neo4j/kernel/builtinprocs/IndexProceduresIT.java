/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.builtinprocs;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.extension.Inject;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
    private static final String labels = "['" + label + "']";
    private static final String properties = "['" + prop + "']";
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

    @Test
    void shouldCreateIndexWithConfig()
    {
        // todo parameterize this test when unique property constraint and node key constraint procedures also can handle index config
        // Given no indexes initially
        assertNoIndexes();

        // When creating index / constraint with config
        Map<IndexSetting,Object> expectedIndexConfiguration = new HashMap<>();
        expectedIndexConfiguration.put( IndexSetting.SPATIAL_CARTESIAN_MAX_LEVELS, 5 );
        expectedIndexConfiguration.put( IndexSetting.SPATIAL_CARTESIAN_MIN, new double[]{-45.0, -45.0} );
        try ( Transaction tx = db.beginTx() )
        {
            String configString = asConfigString( expectedIndexConfiguration );
            tx.execute( asProcedureCall( CREATE_INDEX_FORMAT, "some name", configString ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();

        // Then we should be able to find the index by that name and it should have correct config.
        try ( Transaction tx = db.beginTx() )
        {
            final List<IndexDefinition> indexes = Iterables.asList( tx.schema().getIndexes() );
            assertEquals( 1, indexes.size() );
            final IndexDefinition index = indexes.get( 0 );
            assertEquals( "some name", index.getName() );
            assertEquals( label, single( index.getLabels() ) );
            assertEquals( prop, single( index.getPropertyKeys() ) );
            final Map<IndexSetting,Object> actualIndexConfiguration = index.getIndexConfiguration();
            assertEquals(
                    expectedIndexConfiguration.get( IndexSetting.SPATIAL_CARTESIAN_MAX_LEVELS ),
                    actualIndexConfiguration.get( IndexSetting.SPATIAL_CARTESIAN_MAX_LEVELS ) );
            assertArrayEquals(
                    (double[]) expectedIndexConfiguration.get( IndexSetting.SPATIAL_CARTESIAN_MIN ),
                    (double[]) actualIndexConfiguration.get( IndexSetting.SPATIAL_CARTESIAN_MIN ) );
            tx.commit();
        }
    }

    @Test
    void shouldNotCreateIndexWithNonExistingSetting()
    {
        // todo parameterize this test when unique property constraint and node key constraint procedures also can handle index config
        try ( Transaction tx = db.beginTx() )
        {
            String configString = "{non_existing_setting: 5}";
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( asProcedureCall( CREATE_INDEX_FORMAT, "some name", configString ) ) );
            final Throwable rootCause = getRootCause( e );
            assertTrue( rootCause instanceof IllegalArgumentException );
            assertThat( rootCause.getMessage(),
                    containsString( "Invalid index config key 'non_existing_setting', it was not recognized as an index setting." ) );
        }
        assertNoIndexes();
    }

    @Test
    void shouldNotCreateIndexWithSettingWithWrongValueType()
    {
        // todo parameterize this test when unique property constraint and node key constraint procedures also can handle index config
        try ( Transaction tx = db.beginTx() )
        {
            Map<IndexSetting,Object> config = new HashMap<>();
            config.put( IndexSetting.SPATIAL_CARTESIAN_MAX_LEVELS, "'not_applicable_type'" );
            final String configString = asConfigString( config );
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( asProcedureCall( CREATE_INDEX_FORMAT, "some name", configString ) ) );
            final String asString = Exceptions.stringify( e );
            assertThat( asString,
                    containsString( "Caused by: java.lang.IllegalArgumentException: Could not parse value 'not_applicable_type' of type String as integer." ) );
        }
        assertNoIndexes();
    }

    @Test
    void shouldNotCreateIndexWithSettingWithNullValue()
    {
        // todo parameterize this test when unique property constraint and node key constraint procedures also can handle index config
        try ( Transaction tx = db.beginTx() )
        {
            Map<IndexSetting,Object> config = new HashMap<>();
            config.put( IndexSetting.SPATIAL_CARTESIAN_MAX_LEVELS, null );
            final String configString = asConfigString( config );
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( asProcedureCall( CREATE_INDEX_FORMAT, "some name", configString ) ) );
            final Throwable rootCause = getRootCause( e );
            assertTrue( rootCause instanceof NullPointerException );
            assertThat( rootCause.getMessage(), containsString( "Index setting value can not be null." ) );
        }
        assertNoIndexes();
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
        assertNoIndexes();

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
        return asProcedureCall( procedureName, indexName, "{}" );
    }

    private static String asProcedureCall( String procedureName, String indexName, String config )
    {
        if ( indexName == null )
        {
            return format( "CALL " + procedureName + "( NULL, %s, %s, \"%s\", %s )", labels, properties, providerName, config );
        }
        else
        {
            return format( "CALL " + procedureName + "( \"%s\", %s, %s, \"%s\", %s )", indexName, labels, properties, providerName, config );
        }
    }

    private static String asConfigString( Map<IndexSetting,Object> indexConfiguration )
    {
        StringJoiner joiner = new StringJoiner( ", ", "{", "}" );
        for ( Map.Entry<IndexSetting,Object> entry : indexConfiguration.entrySet() )
        {
            String valueString;
            final Object value = entry.getValue();
            if ( value == null )
            {
                valueString = "null";
            }
            else if ( value instanceof double[] )
            {
                valueString = Arrays.toString( (double[]) value );
            }
            else
            {
                valueString = value.toString();
            }
            joiner.add( "`" + entry.getKey().getSettingName() + "`: " + valueString );
        }
        return joiner.toString();
    }

    private void assertNoIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            final Iterator<IndexDefinition> indexes = tx.schema().getIndexes().iterator();
            assertFalse( indexes.hasNext() );
            tx.commit();
        }
    }
}
