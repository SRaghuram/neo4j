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
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.test.extension.Inject;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    private static final String NO_CONFIG = "{}";
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
    void shouldNotCreateIndexWithFulltextProvider( String procedure )
    {
        // Given no indexes initially
        assertNoIndexes();

        // When trying to create index with fulltext provider
        try ( Transaction tx = db.beginTx() )
        {
            final String fulltextProviderName = FulltextIndexProviderFactory.DESCRIPTOR.name();
            final QueryExecutionException e = assertThrows( QueryExecutionException.class,
                    () -> tx.execute( asProcedureCall( procedure, "some name", fulltextProviderName, NO_CONFIG ) ) );
            final Throwable rootCause = getRootCause( e );
            assertThat( rootCause, instanceOf( ProcedureException.class ) );
            assertThat( rootCause.getMessage(), containsString(
                    "Could not create index with specified index provider 'fulltext-1.0'. To create fulltext index, please use " +
                            "'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'." ) );
        }

        // Then we should not have any new index
        assertNoIndexes();
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldNotCreateIndexWithNonExistingProvider( String procedure )
    {
        // Given no indexes initially
        assertNoIndexes();

        // When trying to create index with fulltext provider
        try ( Transaction tx = db.beginTx() )
        {
            final String nonExistingProvider = "non-existing-provider";
            final QueryExecutionException e = assertThrows( QueryExecutionException.class,
                    () -> tx.execute( asProcedureCall( procedure, "some name", nonExistingProvider, NO_CONFIG ) ) );
            final Throwable rootCause = getRootCause( e );
            assertThat( rootCause, instanceOf( IndexProviderNotFoundException.class ) );
            assertThat( rootCause.getMessage(),
                    containsString( "Tried to get index provider with name non-existing-provider whereas available providers in this session being " ) );
        }

        // Then we should not have any new index
        assertNoIndexes();
    }

    @Test
    void shouldCreateIndexWithConfig()
    {
        // todo parameterize this test when unique property constraint and node key constraint procedures also can handle index config
        // Given no indexes initially
        assertNoIndexes();

        // When creating index / constraint with config
        Map<IndexSetting,Object> expectedIndexConfiguration = new HashMap<>();
        expectedIndexConfiguration.put( IndexSetting.SPATIAL_WGS84_MAX, new double[]{90.0, 90.0} );
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
            assertArrayEquals(
                    (double[]) expectedIndexConfiguration.get( IndexSetting.SPATIAL_WGS84_MAX ),
                    (double[]) actualIndexConfiguration.get( IndexSetting.SPATIAL_WGS84_MAX ) );
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
            assertThat( rootCause, instanceOf( IllegalArgumentException.class ) );
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
            config.put( IndexSetting.SPATIAL_WGS84_MAX, "'not_applicable_type'" );
            final String configString = asConfigString( config );
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( asProcedureCall( CREATE_INDEX_FORMAT, "some name", configString ) ) );
            final String asString = Exceptions.stringify( e );
            assertThat( asString,
                    containsString( "Caused by: java.lang.IllegalArgumentException: Could not parse value 'not_applicable_type' as double[]." ) );
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
            config.put( IndexSetting.SPATIAL_WGS84_MAX, null );
            final String configString = asConfigString( config );
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( asProcedureCall( CREATE_INDEX_FORMAT, "some name", configString ) ) );
            final Throwable rootCause = getRootCause( e );
            assertThat( rootCause, instanceOf( NullPointerException.class ) );
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
        assertThat( rootCause, instanceOf( IllegalArgumentException.class ) );
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
        return asProcedureCall( procedureName, indexName, NO_CONFIG );
    }

    private static String asProcedureCall( String procedureName, String indexName, String config )
    {
        return asProcedureCall( procedureName, indexName, providerName, config );
    }

    private static String asProcedureCall( String procedureName, String indexName, String indexProviderName, String config )
    {
        if ( indexName == null )
        {
            return format( "CALL " + procedureName + "( NULL, %s, %s, \"%s\", %s )", labels, properties, indexProviderName, config );
        }
        else
        {
            return format( "CALL " + procedureName + "( \"%s\", %s, %s, \"%s\", %s )", indexName, labels, properties, indexProviderName, config );
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
