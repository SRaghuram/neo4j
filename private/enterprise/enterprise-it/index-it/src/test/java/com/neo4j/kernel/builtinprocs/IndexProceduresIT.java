/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.builtinprocs;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.AnalyzerProvider;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.service.Services;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.asArray;
import static org.neo4j.internal.helpers.collection.Iterables.single;

@ImpermanentEnterpriseDbmsExtension
@ExtendWith( RandomExtension.class )
class IndexProceduresIT
{
    @Inject
    RandomRule random;
    @Inject
    GraphDatabaseService db;
    private static final String CREATE_INDEX_FORMAT = "db.createIndex";
    private static final String CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT = "db.createUniquePropertyConstraint";
    private static final String CREATE_NODE_KEY_CONSTRAINT_FORMAT = "db.createNodeKey";
    private static final String SCHEMA_STATEMENTS = "db.schemaStatements()";
    private static final Label label = Label.label( "Label" );
    private static final Label label2 = Label.label( "Label2" );
    private static final Label labelWhitespace = Label.label( "Label 3" );
    private static final RelationshipType relType = RelationshipType.withName( "relType" );
    private static final RelationshipType relType2 = RelationshipType.withName( "relType2" );
    private static final RelationshipType relTypeWhitespace = RelationshipType.withName( "relType 3" );
    private static final String prop = "prop";
    private static final String prop2 = "prop2";
    private static final String propWhitespace = "prop 3";
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
                    () -> tx.execute( createSchemaProcedureCall( procedure, "some name", fulltextProviderName, NO_CONFIG ) ) );
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
                    () -> tx.execute( createSchemaProcedureCall( procedure, "some name", nonExistingProvider, NO_CONFIG ) ) );
            final Throwable rootCause = getRootCause( e );
            assertThat( rootCause, instanceOf( IndexProviderNotFoundException.class ) );
            assertThat( rootCause.getMessage(),
                    containsString( "Tried to get index provider with name non-existing-provider whereas available providers in this session being " ) );
        }

        // Then we should not have any new index
        assertNoIndexes();
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldCreateIndexWithConfig( String procedure )
    {
        // Given no indexes initially
        assertNoIndexes();

        // When creating index / constraint with config
        Map<IndexSetting,Object> expectedIndexConfiguration = new HashMap<>();
        expectedIndexConfiguration.put( IndexSetting.SPATIAL_WGS84_MAX, new double[]{90.0, 90.0} );
        expectedIndexConfiguration.put( IndexSetting.SPATIAL_CARTESIAN_MIN, new double[]{-45.0, -45.0} );
        try ( Transaction tx = db.beginTx() )
        {
            String configString = asConfigString( expectedIndexConfiguration );
            tx.execute( createSchemaProcedureCall( procedure, "some name", configString ) ).close();
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

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldNotCreateIndexWithNonExistingSetting( String procedure )
    {
        try ( Transaction tx = db.beginTx() )
        {
            String configString = "{non_existing_setting: 5}";
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( createSchemaProcedureCall( procedure, "some name", configString ) ) );
            final Throwable rootCause = getRootCause( e );
            assertThat( rootCause, instanceOf( IllegalArgumentException.class ) );
            assertThat( rootCause.getMessage(),
                    containsString( "Invalid index config key 'non_existing_setting', it was not recognized as an index setting." ) );
        }
        assertNoIndexes();
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldNotCreateIndexWithSettingWithWrongValueType( String procedure )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<IndexSetting,Object> config = new HashMap<>();
            config.put( IndexSetting.SPATIAL_WGS84_MAX, "'not_applicable_type'" );
            final String configString = asConfigString( config );
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( createSchemaProcedureCall( procedure, "some name", configString ) ) );
            final String asString = Exceptions.stringify( e );
            assertThat( asString,
                    containsString( "Caused by: java.lang.IllegalArgumentException: Could not parse value 'not_applicable_type' as double[]." ) );
        }
        assertNoIndexes();
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldNotCreateIndexWithSettingWithNullValue( String procedure )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<IndexSetting,Object> config = new HashMap<>();
            config.put( IndexSetting.SPATIAL_WGS84_MAX, null );
            final String configString = asConfigString( config );
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( createSchemaProcedureCall( procedure, "some name", configString ) ) );
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
                        tx.execute( createSchemaProcedureCall( procedure, "" ) ).close();
                        tx.commit();
                    }
                }
        );
        final Throwable rootCause = getRootCause( exception );
        assertThat( rootCause, instanceOf( IllegalArgumentException.class ) );
        assertEquals( "Schema rule name cannot be the empty string or only contain whitespace.", rootCause.getMessage() );
    }

    @Test
    void schemaStatementsMustDropAndRecreateAllIndexes()
    {
        // Verify that we do not have any indexes initially
        assertNoIndexes();

        // Create a bunch of indexes
        List<UnboundIndexDefinition> allIndexes = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( label ).on( prop )
                    .withName( "btree" )
                    .withIndexType( IndexType.BTREE )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( label ).on( prop ).on( prop2 )
                    .withName( "btree composite" )
                    .withIndexType( IndexType.BTREE )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( labelWhitespace ).on( propWhitespace )
                    .withName( "btree whitespace" )
                    .withIndexType( IndexType.BTREE )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( label ).on( prop )
                    .withName( "full-text" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( labelWhitespace ).on( propWhitespace )
                    .withName( "full-text whitespace" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( label, label2 ).on( prop )
                    .withName( "full-text multi-label" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( label ).on( prop ).on( prop2 )
                    .withName( "full-text multi-prop" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( relType ).on( prop )
                    .withName( "relType full-text" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( relTypeWhitespace ).on( propWhitespace )
                    .withName( "relType full-text whitespace" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( relType, relType2 ).on( prop )
                    .withName( "relType full-text multi-label" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create() ) );
            allIndexes.add( new UnboundIndexDefinition( tx.schema().indexFor( relType ).on( prop ).on( prop2 )
                    .withName( "relType full-text multi-prop" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create() ) );
            tx.commit();
        }

        Map<String,Map<String,Object>> indexNameToSchemaStatements = callSchemaStatements();
        verifySchemaStatementsHasResultForAll( allIndexes, indexNameToSchemaStatements );
        dropAllFromSchemaStatements( indexNameToSchemaStatements );
        assertNoIndexes();
        recreateAllFromSchemaStatements( indexNameToSchemaStatements );
        verifyHasCopyOfIndexes( allIndexes );
    }

    //todo
    // schemaStatementsMustDropAndRecreateAllConstraints
    // schemaStatementsDontListIndexOwnedByConstraint
    // ---Special cases---
    // schemaStatementsOrphanedIndexes
    // schemaStatementsFailedIndexes
    // schemaStatementsPopulatingIndexes

    private void verifySchemaStatementsHasResultForAll( List<UnboundIndexDefinition> allIndexes,
            Map<String,Map<String,Object>> indexNameToSchemaStatements )
    {
        final Set<String> indexNames = indexNameToSchemaStatements.keySet();
        for ( UnboundIndexDefinition index : allIndexes )
        {
            assertTrue( indexNames.contains( index.name ), "Expected schemaStatements to include all indexes but was missing " + index.name );
        }
    }

    private void dropAllFromSchemaStatements( Map<String,Map<String,Object>> indexNameToSchemaStatements )
    {
        for ( Map.Entry<String,Map<String,Object>> schemaStatements : indexNameToSchemaStatements.entrySet() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                final Map<String,Object> schemaStatementResult = schemaStatements.getValue();
                tx.execute( getDropStatement( schemaStatementResult ) ).close();
                tx.commit();
            }
        }
    }

    private void recreateAllFromSchemaStatements( Map<String,Map<String,Object>> indexNameToSchemaStatements )
    {
        for ( Map.Entry<String,Map<String,Object>> schemaStatements : indexNameToSchemaStatements.entrySet() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                final Map<String,Object> schemaStatementResult = schemaStatements.getValue();
                final String createStatement = getCreateStatement( schemaStatementResult );
                tx.execute( createStatement ).close();
                tx.commit();
            }
        }
    }

    private void verifyHasCopyOfIndexes( List<UnboundIndexDefinition> allIndexes )
    {
        Map<String,UnboundIndexDefinition> recreatedIndexes = new HashMap<>();
        try ( Transaction tx = db.beginTx() )
        {
            Iterable<IndexDefinition> indexes = tx.schema().getIndexes();
            for ( IndexDefinition index : indexes )
            {
                recreatedIndexes.put( index.getName(), new UnboundIndexDefinition( index ) );
            }
            tx.commit();
        }
        for ( UnboundIndexDefinition originalIndex : allIndexes )
        {
            final UnboundIndexDefinition recreatedIndex = recreatedIndexes.remove( originalIndex.name );
            assertNotNull( recreatedIndex );
            assertIndexDefinitionsEqual( originalIndex, recreatedIndex );
        }
        assertEquals( 0, recreatedIndexes.size() );
    }

    private Map<String,Map<String,Object>> callSchemaStatements()
    {
        Map<String,Map<String,Object>> indexNameToSchemaStatements = new HashMap<>();
        try ( Transaction tx = db.beginTx() )
        {
            final Result result = tx.execute( "CALL " + SCHEMA_STATEMENTS );
            while ( result.hasNext() )
            {
                final Map<String,Object> next = result.next();
                indexNameToSchemaStatements.put( (String) next.get( "name" ), next );
            }
            result.close();
            tx.commit();
        }
        return indexNameToSchemaStatements;
    }

    private static void assertIndexDefinitionsEqual( UnboundIndexDefinition expected, UnboundIndexDefinition actual )
    {
        assertEquals( expected.name, actual.name );
        assertEquals( expected.indexType, actual.indexType );
        if ( expected.isNodeIndex )
        {
            assertTrue( actual.isNodeIndex );
            assertArrayEquals( expected.labels, actual.labels );
        }
        if ( expected.isRelationshipIndex )
        {
            assertTrue( actual.isRelationshipIndex );
            assertArrayEquals( expected.relationshipTypes, actual.relationshipTypes );
        }
        assertArrayEquals( expected.propertyKeys, actual.propertyKeys );
        assertEqualConfig( expected, actual );
    }

    private static void assertEqualConfig( UnboundIndexDefinition expected, UnboundIndexDefinition actual )
    {
        final Map<IndexSetting,Object> expectedConfig = expected.config;
        final Map<IndexSetting,Object> actualConfig = actual.config;
        assertEquals( expectedConfig.size(), actualConfig.size() );
        for ( Map.Entry<IndexSetting,Object> expectedEntry : expectedConfig.entrySet() )
        {
            final IndexSetting key = expectedEntry.getKey();
            final Object expectedValue = expectedEntry.getValue();
            final Object actualValue = actualConfig.get( key );
            assertNotNull( actualValue );
            if ( expectedValue instanceof double[] )
            {
                assertArrayEquals( (double[]) expectedValue, (double[]) actualValue );
            }
            else
            {
                assertEquals( expectedValue, actualValue );
            }
        }
    }

    private String getCreateStatement( Map<String,Object> schemaStatementResult )
    {
        return (String) schemaStatementResult.get( "createStatement" );
    }

    private String getDropStatement( Map<String,Object> schemaStatementResult )
    {
        return (String) schemaStatementResult.get( "dropStatement" );
    }

    private Map<IndexSetting,Object> randomFulltextSettings()
    {
        Map<IndexSetting,Object> indexConfiguration = new HashMap<>();
        indexConfiguration.put( IndexSetting.FULLTEXT_EVENTUALLY_CONSISTENT, random.nextBoolean() );
        indexConfiguration.put( IndexSetting.FULLTEXT_ANALYZER, randomAnalyzer() );
        return indexConfiguration;
    }

    private String randomAnalyzer()
    {
        final List<AnalyzerProvider> analyzers = new ArrayList<>( Services.loadAll( AnalyzerProvider.class ) );
        return random.randomValues().among( analyzers ).getName();
    }

    private Map<IndexSetting,Object> randomBtreeSettings()
    {
        Map<IndexSetting,Object> indexConfiguration = new HashMap<>();
        for ( IndexSetting indexSetting : IndexSetting.values() )
        {
            if ( indexSetting.getSettingName().startsWith( "spatial" ) )
            {
                indexConfiguration.put( indexSetting, randomSpatialValue( indexSetting ) );
            }
        }
        return indexConfiguration;
    }

    private double[] randomSpatialValue( IndexSetting indexSetting )
    {
        switch ( indexSetting )
        {
        case SPATIAL_CARTESIAN_MIN:
            return negative( random.randomValues().nextCartesianPoint().coordinate() );
        case SPATIAL_CARTESIAN_MAX:
            return positive( random.randomValues().nextCartesianPoint().coordinate() );
        case SPATIAL_CARTESIAN_3D_MIN:
            return negative( random.randomValues().nextCartesian3DPoint().coordinate() );
        case SPATIAL_CARTESIAN_3D_MAX:
            return positive( random.randomValues().nextCartesian3DPoint().coordinate() );
        case SPATIAL_WGS84_MIN:
            return negative( random.randomValues().nextGeographicPoint().coordinate() );
        case SPATIAL_WGS84_MAX:
            return positive( random.randomValues().nextGeographicPoint().coordinate() );
        case SPATIAL_WGS84_3D_MIN:
            return negative( random.randomValues().nextGeographic3DPoint().coordinate() );
        case SPATIAL_WGS84_3D_MAX:
            return positive( random.randomValues().nextGeographic3DPoint().coordinate() );
        case FULLTEXT_ANALYZER:
        case FULLTEXT_EVENTUALLY_CONSISTENT:
        default:
            throw new IllegalArgumentException( "no" );
        }
    }

    private static double[] positive( double[] values )
    {
        final double[] result = new double[values.length];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = Math.abs( values[i] );
        }
        return result;
    }

    private static double[] negative( double[] values )
    {
        final double[] result = new double[values.length];
        for ( int i = 0; i < values.length; i++ )
        {
            result[i] = -Math.abs( values[i] );
        }
        return result;
    }

    private void shouldCreateIndexWithName( String procedure, String indexName )
    {
        // Given no indexes initially
        assertNoIndexes();

        // When creating index / constraint with name
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( createSchemaProcedureCall( procedure, indexName ) ).close();
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

    private static String createSchemaProcedureCall( String procedureName, String indexName )
    {
        return createSchemaProcedureCall( procedureName, indexName, NO_CONFIG );
    }

    private static String createSchemaProcedureCall( String procedureName, String indexName, String config )
    {
        return createSchemaProcedureCall( procedureName, indexName, providerName, config );
    }

    private static String createSchemaProcedureCall( String procedureName, String indexName, String indexProviderName, String config )
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

    private static class UnboundIndexDefinition
    {
        private final String name;
        private final IndexType indexType;
        private final boolean isNodeIndex;
        private final boolean isRelationshipIndex;
        private final Label[] labels;
        private final RelationshipType[] relationshipTypes;
        private final String[] propertyKeys;
        private final Map<IndexSetting,Object> config;

        private UnboundIndexDefinition( IndexDefinition indexDefinition )
        {
            this.name = indexDefinition.getName();
            this.indexType = indexDefinition.getIndexType();
            this.isNodeIndex = indexDefinition.isNodeIndex();
            this.isRelationshipIndex = indexDefinition.isRelationshipIndex();
            this.labels = isNodeIndex ? asArray( Label.class, indexDefinition.getLabels() ) : null;
            this.relationshipTypes = isRelationshipIndex ? asArray( RelationshipType.class, indexDefinition.getRelationshipTypes() ) : null;
            this.propertyKeys = asArray( String.class, indexDefinition.getPropertyKeys() );
            this.config = indexDefinition.getIndexConfiguration();
        }
    }
}
