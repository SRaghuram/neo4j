/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.AnalyzerProvider;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingImpl;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_ANALYZER;
import static org.neo4j.graphdb.schema.IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MIN;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MAX;
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
    private static final Label label3 = Label.label( "Label3" );
    private static final Label labelWhitespace = Label.label( "Label 1" );
    private static final Label labelWhitespace2 = Label.label( "Label 2" );
    private static final Label labelWhitespace3 = Label.label( "Label 3" );
    private static final Label labelBackticks = Label.label( "`Label``4`" );
    private static final RelationshipType relType = RelationshipType.withName( "relType" );
    private static final RelationshipType relType2 = RelationshipType.withName( "relType2" );
    private static final RelationshipType relTypeWhitespace = RelationshipType.withName( "relType 3" );
    private static final RelationshipType relTypeBackticks = RelationshipType.withName( "`rel`type`" );
    private static final String prop = "prop";
    private static final String prop2 = "prop2";
    private static final String prop3 = "prop3";
    private static final String propWhitespace = "prop 1";
    private static final String propWhitespace2 = "prop 2";
    private static final String propWhitespace3 = "prop 3";
    private static final String propBackticks = "``prop`4`";
    private static final String propBackticks2 = "`prop5``";
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
        assertNoSchemaRules();

        // When trying to create index with fulltext provider
        try ( Transaction tx = db.beginTx() )
        {
            final String fulltextProviderName = FulltextIndexProviderFactory.DESCRIPTOR.name();
            final QueryExecutionException e = assertThrows( QueryExecutionException.class,
                    () -> tx.execute( createSchemaProcedureCall( procedure, "some name", fulltextProviderName, NO_CONFIG ) ) );
            final Throwable rootCause = getRootCause( e );
            assertThat( rootCause ).isInstanceOf( ProcedureException.class );
            assertThat( rootCause.getMessage() ).contains(
                    "Could not create index with specified index provider 'fulltext-1.0'. To create fulltext index, please use " +
                            "'db.index.fulltext.createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'." );
        }

        // Then we should not have any new index
        assertNoSchemaRules();
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldNotCreateIndexWithNonExistingProvider( String procedure )
    {
        // Given no indexes initially
        assertNoSchemaRules();

        // When trying to create index with fulltext provider
        try ( Transaction tx = db.beginTx() )
        {
            final String nonExistingProvider = "non-existing-provider";
            final QueryExecutionException e = assertThrows( QueryExecutionException.class,
                    () -> tx.execute( createSchemaProcedureCall( procedure, "some name", nonExistingProvider, NO_CONFIG ) ) );
            final Throwable rootCause = getRootCause( e );
            assertThat( rootCause ).isInstanceOf( IndexProviderNotFoundException.class );
            assertThat( rootCause.getMessage() ).contains(
                    "Tried to get index provider with name non-existing-provider whereas available providers in this session being " );
        }

        // Then we should not have any new index
        assertNoSchemaRules();
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldCreateIndexWithConfig( String procedure )
    {
        // Given no indexes initially
        assertNoSchemaRules();

        // When creating index / constraint with config
        Map<IndexSetting,Object> expectedIndexConfiguration = new HashMap<>();
        expectedIndexConfiguration.put( SPATIAL_WGS84_MAX, new double[]{90.0, 90.0} );
        expectedIndexConfiguration.put( SPATIAL_CARTESIAN_MIN, new double[]{-45.0, -45.0} );
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
                    (double[]) expectedIndexConfiguration.get( SPATIAL_WGS84_MAX ),
                    (double[]) actualIndexConfiguration.get( SPATIAL_WGS84_MAX ) );
            assertArrayEquals(
                    (double[]) expectedIndexConfiguration.get( SPATIAL_CARTESIAN_MIN ),
                    (double[]) actualIndexConfiguration.get( SPATIAL_CARTESIAN_MIN ) );
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
            assertThat( rootCause ).isInstanceOf( IllegalArgumentException.class );
            assertThat( rootCause.getMessage() ).contains( "Invalid index config key 'non_existing_setting', it was not recognized as an index setting." );
        }
        assertNoSchemaRules();
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldNotCreateIndexWithSettingWithWrongValueType( String procedure )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<IndexSetting,Object> config = new HashMap<>();
            config.put( SPATIAL_WGS84_MAX, "'not_applicable_type'" );
            final String configString = asConfigString( config );
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( createSchemaProcedureCall( procedure, "some name", configString ) ) );
            final String asString = Exceptions.stringify( e );
            assertThat( asString ).contains( "Caused by: org.neo4j.graphdb.schema.IndexSettingUtil$IndexSettingParseException: " +
                    "Could not parse value 'not_applicable_type' as double[]." );
        }
        assertNoSchemaRules();
    }

    @ParameterizedTest
    @ValueSource( strings = {CREATE_INDEX_FORMAT, CREATE_UNIQUE_PROPERTY_CONSTRAINT_FORMAT, CREATE_NODE_KEY_CONSTRAINT_FORMAT} )
    void shouldNotCreateIndexWithSettingWithNullValue( String procedure )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<IndexSetting,Object> config = new HashMap<>();
            config.put( SPATIAL_WGS84_MAX, null );
            final String configString = asConfigString( config );
            final QueryExecutionException e =
                    assertThrows( QueryExecutionException.class, () -> tx.execute( createSchemaProcedureCall( procedure, "some name", configString ) ) );
            final Throwable rootCause = getRootCause( e );
            assertThat( rootCause ).isInstanceOf( NullPointerException.class );
            assertThat( rootCause.getMessage() ).contains( "Index setting value can not be null." );
        }
        assertNoSchemaRules();
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
        assertThat( rootCause ).isInstanceOf( IllegalArgumentException.class );
        assertEquals( "Schema rule name cannot be the empty string or only contain whitespace.", rootCause.getMessage() );
    }

    @Test
    void schemaStatementsMustDropAndRecreateAllIndexes()
    {
        // Verify that we do not have any indexes initially
        assertNoSchemaRules();

        // Create a bunch of indexes
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label ).on( prop )
                    .withName( "btree" )
                    .withIndexType( IndexType.BTREE )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create();
            tx.schema().indexFor( label ).on( prop ).on( prop2 )
                    .withName( "btree composite" )
                    .withIndexType( IndexType.BTREE )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create();
            tx.schema().indexFor( labelWhitespace ).on( propWhitespace )
                    .withName( "btree whitespace" )
                    .withIndexType( IndexType.BTREE )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create();
            tx.schema().indexFor( labelBackticks ).on( propBackticks )
                    .withName( "btree backticks" )
                    .withIndexType( IndexType.BTREE )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create();
            tx.schema().indexFor( label ).on( prop2 )
                    .withName( "``horrible `index`name```" )
                    .withIndexType( IndexType.BTREE )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create();
            tx.schema().indexFor( label ).on( prop )
                    .withName( "full-text" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( labelWhitespace ).on( propWhitespace )
                    .withName( "full-text whitespace" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( label, label2 ).on( prop )
                    .withName( "full-text multi-label" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( label ).on( prop ).on( prop2 )
                    .withName( "full-text multi-prop" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( labelBackticks ).on( propBackticks )
                    .withName( "full-text backticks" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( labelBackticks, label ).on( prop).on( propBackticks )
                    .withName( "advanced full-text backticks" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( label ).on( prop2 )
                    .withName( "``horrible `index`name``2`" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( relType ).on( prop )
                    .withName( "relType full-text" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( relTypeWhitespace ).on( propWhitespace )
                    .withName( "relType full-text whitespace" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( relType, relType2 ).on( prop )
                    .withName( "relType full-text multi-label" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( relType ).on( prop ).on( prop2 )
                    .withName( "relType full-text multi-prop" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( relTypeBackticks ).on( propBackticks )
                    .withName( "relType full-text backticks" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.schema().indexFor( relType ).on( prop2 )
                    .withName( "``horrible `index`name`3``" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( randomFulltextSettings() )
                    .create();
            tx.commit();
        }
        verifyCanDropAndRecreateAllSchemaRulesUsingSchemaStatements();
    }

    @Test
    void schemaStatementsMustDropAndRecreateAllConstraints()
    {
        // Verify that we do not have any schema rules initially
        assertNoSchemaRules();

        // Create a bunch of constraints
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( label ).assertPropertyIsUnique( prop )
                    .withName( "unique property" )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create();
            tx.schema().constraintFor( labelWhitespace ).assertPropertyIsUnique( propWhitespace )
                    .withName( "unique property whitespace" )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create();
            tx.schema().constraintFor( label2 ).assertPropertyIsNodeKey( prop2 )
                    .withName( "node key" )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create();
            tx.schema().constraintFor( labelWhitespace2 ).assertPropertyIsNodeKey( propWhitespace2 )
                    .withName( "node key whitespace" )
                    .withIndexConfiguration( randomBtreeSettings() )
                    .create();
            tx.schema().constraintFor( label3 ).assertPropertyExists( prop3 )
                    .withName( "node prop exists" )
                    .create();
            tx.schema().constraintFor( labelWhitespace3 ).assertPropertyExists( propWhitespace3 )
                    .withName( "node prop exists whitespace" )
                    .create();
            tx.schema().constraintFor( labelBackticks ).assertPropertyExists( propBackticks )
                    .withName( "exists backticks" )
                    .create();
            tx.schema().constraintFor( labelBackticks ).assertPropertyIsUnique( propBackticks )
                    .withName( "unique backticks" )
                    .create();
            tx.schema().constraintFor( labelBackticks ).assertPropertyIsNodeKey( propBackticks2 )
                    .withName( "node key backticks" )
                    .create();
            tx.schema().constraintFor( label2 ).assertPropertyIsUnique( prop )
                    .withName( "``horrible`name`" )
                    .create();
            tx.schema().constraintFor( label3 ).assertPropertyExists( prop2 )
                    .withName( "``horrible`name2`" )
                    .create();
            tx.schema().constraintFor( label ).assertPropertyIsNodeKey( prop2 )
                    .withName( "``horrible`name3`" )
                    .create();
            tx.schema().constraintFor( relType ).assertPropertyExists( prop )
                    .withName( "rel prop exists" )
                    .create();
            tx.schema().constraintFor( relTypeWhitespace ).assertPropertyExists( propWhitespace )
                    .withName( "rel prop exists whitespace" )
                    .create();
            tx.schema().constraintFor( relTypeBackticks ).assertPropertyExists( propBackticks )
                    .withName( "rel prop exists backticks" )
                    .create();
            tx.schema().constraintFor( relType ).assertPropertyExists( prop2 )
                    .withName( "``horrible`name4`" )
                    .create();
            tx.commit();
        }
        verifyCanDropAndRecreateAllSchemaRulesUsingSchemaStatements();
    }

    private void verifyCanDropAndRecreateAllSchemaRulesUsingSchemaStatements()
    {
        awaitIndexesOnline();
        List<UnboundIndexDefinition> allIndexes = allIndexes();
        List<UnboundConstraintDefinition> allConstraints = allConstraints();
        Map<String,Map<String,Object>> schemaRuleNameToSchemaStatements = callSchemaStatements();
        dropAllFromSchemaStatements( schemaRuleNameToSchemaStatements );
        assertNoSchemaRules();
        recreateAllFromSchemaStatements( schemaRuleNameToSchemaStatements );
        verifyHasCopyOfSchemaRules( allIndexes, allConstraints );
    }

    private List<UnboundIndexDefinition> allIndexes()
    {
        List<UnboundIndexDefinition> allIndexes = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().getIndexes().forEach( id -> allIndexes.add( new UnboundIndexDefinition( id ) ) );
            tx.commit();
        }
        return allIndexes;
    }

    private List<UnboundConstraintDefinition> allConstraints()
    {
        List<UnboundConstraintDefinition> allConstraints = new ArrayList<>();
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().getConstraints().forEach( cd -> allConstraints.add( new UnboundConstraintDefinition( cd ) ) );
            tx.commit();
        }
        return allConstraints;
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

    private void verifyHasCopyOfSchemaRules( List<UnboundIndexDefinition> originalIndexes, List<UnboundConstraintDefinition> originalConstraints )
    {
        Map<String,UnboundIndexDefinition> recreatedIndexes = asNameMap( allIndexes() );
        Map<String,UnboundConstraintDefinition> recreatedConstraints = asNameMap( allConstraints() );
        for ( UnboundIndexDefinition originalIndex : originalIndexes )
        {
            final UnboundIndexDefinition recreatedIndex = recreatedIndexes.remove( originalIndex.getName() );
            assertNotNull( recreatedIndex );
            assertIndexDefinitionsEqual( originalIndex, recreatedIndex );
        }
        for ( UnboundConstraintDefinition originalConstraint : originalConstraints )
        {
            final UnboundConstraintDefinition recreatedConstraint = recreatedConstraints.remove( originalConstraint.getName() );
            assertNotNull( recreatedConstraint );
            assertConstraintDefinitionsEqual( originalConstraint, recreatedConstraint );
        }
        assertEquals( 0, recreatedIndexes.size() );
        assertEquals( 0, recreatedConstraints.size() );
    }

    private <T extends UnboundDefinition> Map<String,T> asNameMap( List<T> schemaRules )
    {
        Map<String,T> result = new HashMap<>();
        schemaRules.forEach( sr -> result.put( sr.getName(), sr ) );
        return result;
    }

    private Map<String,Map<String,Object>> callSchemaStatements()
    {
        Map<String,Map<String,Object>> indexNameToSchemaStatements = new HashMap<>();
        try ( Transaction tx = db.beginTx();
              Result result = tx.execute( "CALL " + SCHEMA_STATEMENTS ) )
        {
            while ( result.hasNext() )
            {
                final Map<String,Object> next = result.next();
                indexNameToSchemaStatements.put( (String) next.get( "name" ), next );
            }
            tx.commit();
        }
        return indexNameToSchemaStatements;
    }

    private static void assertDefinitionsEqual( UnboundDefinition expected, UnboundDefinition actual )
    {
        assertEquals( expected.getName(), actual.getName() );
        assertArrayEquals( expected.labels, actual.labels );
        assertArrayEquals( expected.relationshipTypes, actual.relationshipTypes );
        assertArrayEquals( expected.propertyKeys, actual.propertyKeys );
    }

    private static void assertIndexDefinitionsEqual( UnboundIndexDefinition expected, UnboundIndexDefinition actual )
    {
        assertDefinitionsEqual( expected, actual );
        assertEquals( expected.indexType, actual.indexType );
        assertEquals( expected.isNodeIndex, actual.isNodeIndex );
        assertEquals( expected.isRelationshipIndex, actual.isRelationshipIndex );
        assertEqualConfig( expected, actual );
    }

    private static void assertConstraintDefinitionsEqual( UnboundConstraintDefinition expected, UnboundConstraintDefinition actual )
    {
        assertDefinitionsEqual( expected, actual );
        assertEquals( expected.constraintType, actual.constraintType );
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
        indexConfiguration.put( FULLTEXT_EVENTUALLY_CONSISTENT, random.nextBoolean() );
        indexConfiguration.put( FULLTEXT_ANALYZER, randomAnalyzer() );
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
        for ( IndexSettingImpl indexSetting : IndexSettingImpl.values() )
        {
            if ( indexSetting.getSettingName().startsWith( "spatial" ) )
            {
                indexConfiguration.put( indexSetting, randomSpatialValue( indexSetting ) );
            }
        }
        return indexConfiguration;
    }

    private double[] randomSpatialValue( IndexSettingImpl indexSetting )
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
        assertNoSchemaRules();

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

    private void assertNoSchemaRules()
    {
        assertEquals( 0, allConstraints().size() );
        assertEquals( 0, allIndexes().size() );
    }

    private static class UnboundDefinition
    {
        private final String name;
        private final Label[] labels;
        private final RelationshipType[] relationshipTypes;
        private final String[] propertyKeys;

        UnboundDefinition( String name, Label[] labels, RelationshipType[] relationshipTypes, String[] propertyKeys )
        {
            this.name = name;
            this.labels = labels;
            this.relationshipTypes = relationshipTypes;
            this.propertyKeys = propertyKeys;
        }

        String getName()
        {
            return name;
        }
    }

    private static class UnboundIndexDefinition extends UnboundDefinition
    {
        private final IndexType indexType;
        private final boolean isNodeIndex;
        private final boolean isRelationshipIndex;
        private final Map<IndexSetting,Object> config;

        private UnboundIndexDefinition( IndexDefinition indexDefinition )
        {
            super( indexDefinition.getName(),
                    indexDefinition.isNodeIndex() ? asArray( Label.class, indexDefinition.getLabels() ) : null,
                    indexDefinition.isRelationshipIndex() ? asArray( RelationshipType.class, indexDefinition.getRelationshipTypes() ) : null,
                    asArray( String.class, indexDefinition.getPropertyKeys() ) );
            this.indexType = indexDefinition.getIndexType();
            this.isNodeIndex = indexDefinition.isNodeIndex();
            this.isRelationshipIndex = indexDefinition.isRelationshipIndex();
            this.config = indexDefinition.getIndexConfiguration();
        }
    }

    private static class UnboundConstraintDefinition extends UnboundDefinition
    {
        private final ConstraintType constraintType;

        private UnboundConstraintDefinition( ConstraintDefinition constraintDefinition )
        {
            super( constraintDefinition.getName(),
                    constraintDefinition.isConstraintType( RELATIONSHIP_PROPERTY_EXISTENCE ) ? null : new Label[]{constraintDefinition.getLabel()},
                    constraintDefinition.isConstraintType( RELATIONSHIP_PROPERTY_EXISTENCE ) ?
                    new RelationshipType[]{constraintDefinition.getRelationshipType()} : null,
                    asArray( String.class, constraintDefinition.getPropertyKeys() ) );
            this.constraintType = constraintDefinition.getConstraintType();
        }
    }
}
