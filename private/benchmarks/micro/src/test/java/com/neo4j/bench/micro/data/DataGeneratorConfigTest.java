/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.micro.data.DataGenerator.LabelLocality;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGenerator.PropertyLocality;
import com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.micro.data.DiscreteGenerator.discrete;
import static com.neo4j.bench.micro.data.NumberGenerator.ascLong;
import static com.neo4j.bench.micro.data.NumberGenerator.randDouble;
import static com.neo4j.bench.micro.data.NumberGenerator.toDouble;
import static com.neo4j.bench.micro.data.NumberGenerator.toFloat;
import static com.neo4j.bench.micro.data.PointGenerator.ClusterGridDefinition.from;
import static com.neo4j.bench.micro.data.PointGenerator.circleGrid;
import static com.neo4j.bench.micro.data.PointGenerator.clusterGrid;
import static com.neo4j.bench.micro.data.PointGenerator.diagonal;
import static com.neo4j.bench.micro.data.PointGenerator.grid;
import static com.neo4j.bench.micro.data.TemporalGenerator.date;
import static com.neo4j.bench.micro.data.TemporalGenerator.dateTime;
import static com.neo4j.bench.micro.data.TemporalGenerator.duration;
import static com.neo4j.bench.micro.data.TemporalGenerator.localDatetime;
import static com.neo4j.bench.micro.data.TemporalGenerator.localTime;
import static com.neo4j.bench.micro.data.TemporalGenerator.time;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.FLT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.LNG_ARR;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_BIG;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.STR_SML;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.ascPropertyFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.discreteBucketsFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@TestDirectoryExtension
public class DataGeneratorConfigTest
{
    @Inject
    public TestDirectory temporaryFolder;

    @Test
    public void fullySpecifiedDataGeneratorConfigurationsShouldBeEqual() throws IOException
    {
        Neo4jConfig neo4jConfig = new Neo4jConfig( MapUtil.genericMap(
                "a", "1",
                "b", "\"2\"",
                "c", "3",
                "d", "a/b/c/d.e",
                "e", "true" ) );

        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNodeCount( 1 )
                .withOutRelationships(
                        new RelationshipDefinition( RelationshipType.withName( "REL1" ), 1 ),
                        new RelationshipDefinition( RelationshipType.withName( "REL2" ), 2 ) )
                .withRelationshipLocality( RelationshipLocality.SCATTERED_BY_START_NODE )
                .withNodeProperties(
                        new PropertyDefinition( "n1", randPropertyFor( STR_BIG ).value() ),
                        new PropertyDefinition( "n2", discrete( discreteBucketsFor( INT, 1, 2, 3 ) ) ),
                        new PropertyDefinition( "n3", toFloat( discrete( discreteBucketsFor( INT, 1, 2, 3 ) ) ) ) )
                .withRelationshipProperties(
                        new PropertyDefinition( "r1", randPropertyFor( DBL_ARR ).value() ),
                        new PropertyDefinition( "r2", discrete( discreteBucketsFor( LNG, 1, 2, 3 ) ) ),
                        new PropertyDefinition( "r3", toDouble( discrete( discreteBucketsFor( LNG, 1, 2, 3 ) ) ) ) )
                .withPropertyOrder( Order.ORDERED )
                .withPropertyLocality( PropertyLocality.SCATTERED_BY_ELEMENT )
                .withLabels( Label.label( "label1" ) )
                .withLabelOrder( Order.SHUFFLED )
                .withLabelLocality( LabelLocality.SCATTERED_BY_NODE )
                .withSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .withUniqueConstraints(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .withMandatoryNodeConstraints(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .withMandatoryRelationshipConstraints(
                        new RelationshipKeyDefinition( RelationshipType.withName( "REL1" ), "C" ),
                        new RelationshipKeyDefinition( RelationshipType.withName( "REL2" ), "D" ) )
                .withFulltextNodeSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .withFulltextRelationshipSchemaIndexes(
                        new RelationshipKeyDefinition( RelationshipType.withName( "REL1" ), "C" ),
                        new RelationshipKeyDefinition( RelationshipType.withName( "REL2" ), "D" ) )
                .withNeo4jConfig( neo4jConfig )
                .withRngSeed( 42 )
                .isReusableStore( true )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void configurationsWithReusableTrueShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void configurationsWithDifferentValuesForReusableShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( false )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void augmentedConfigurationsShouldNotBeEqualWhenKeysAreDifferent() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder().augmentedBy( "yin" ).build();
        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder().augmentedBy( "yang" ).build();
        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void augmentedConfigurationsShouldBeEqualWhenKeysAreSame() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder().augmentedBy( "same" ).build();
        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder().augmentedBy( "same" ).build();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void unAugmentedConfigurationsShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .augmentedBy( "SomeBenchmark" )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void emptyConfigurationsWithReusableFalseShouldBeEqualWhenIgnoringStoreId() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( false )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();

        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void nodeCountDataGeneratorConfigurationsShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNodeCount( 1 )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void nodeCountDataGeneratorConfigurationsShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNodeCount( 1 )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNodeCount( 2 )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void relationshipCountDataGeneratorConfigurationsShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "REL" ), 1 ) )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void relationshipCountDataGeneratorConfigurationsShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "REL1" ), 1 ) )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "REL1" ), 2 ) )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void scatteredRelationshipsDataGeneratorConfigurationsShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withRelationshipLocality( RelationshipLocality.SCATTERED_BY_START_NODE )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void scatteredRelationshipsDataGeneratorConfigurationsShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withRelationshipLocality( RelationshipLocality.SCATTERED_BY_START_NODE )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withRelationshipLocality( RelationshipLocality.CO_LOCATED_BY_START_NODE )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void configurationsForAllPropertyTypesShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNodeProperties(
                        new PropertyDefinition( "rand1", randPropertyFor( STR_SML ).value() ),
                        new PropertyDefinition( "rand2", randPropertyFor( STR_BIG ).value() ),
                        new PropertyDefinition( "rand3", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "rand4", randPropertyFor( LNG ).value() ),
                        new PropertyDefinition( "rand5", randPropertyFor( FLT ).value() ),
                        new PropertyDefinition( "rand6", randPropertyFor( DBL ).value() ),
                        new PropertyDefinition( "rand7", randPropertyFor( INT_ARR ).value() ),
                        new PropertyDefinition( "rand8", randPropertyFor( LNG_ARR ).value() ),
                        new PropertyDefinition( "rand9", randPropertyFor( FLT_ARR ).value() ),
                        new PropertyDefinition( "rand10", randPropertyFor( DBL_ARR ).value() ),
                        new PropertyDefinition( "asc1", ascPropertyFor( STR_SML ).value() ),
                        new PropertyDefinition( "asc2", ascPropertyFor( STR_BIG ).value() ),
                        new PropertyDefinition( "asc3", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "asc4", ascPropertyFor( LNG ).value() ),
                        new PropertyDefinition( "asc5", ascPropertyFor( FLT ).value() ),
                        new PropertyDefinition( "asc6", ascPropertyFor( DBL ).value() ),
                        new PropertyDefinition( "asc7", ascPropertyFor( INT_ARR ).value() ),
                        new PropertyDefinition( "asc8", ascPropertyFor( LNG_ARR ).value() ),
                        new PropertyDefinition( "asc9", ascPropertyFor( FLT_ARR ).value() ),
                        new PropertyDefinition( "asc10", ascPropertyFor( DBL_ARR ).value() ),
                        new PropertyDefinition( "dis1", discrete( discreteBucketsFor( STR_SML, 1, 2 ) ) ),
                        new PropertyDefinition( "dis2", discrete( discreteBucketsFor( STR_BIG, 1, 2 ) ) ),
                        new PropertyDefinition( "dis3", discrete( discreteBucketsFor( INT, 1, 2 ) ) ),
                        new PropertyDefinition( "dis4", discrete( discreteBucketsFor( LNG, 1, 2 ) ) ),
                        new PropertyDefinition( "dis5", discrete( discreteBucketsFor( FLT, 1, 2 ) ) ),
                        new PropertyDefinition( "dis6", discrete( discreteBucketsFor( DBL, 1, 2 ) ) ),
                        new PropertyDefinition( "dis7", discrete( discreteBucketsFor( INT_ARR, 1, 2 ) ) ),
                        new PropertyDefinition( "dis8", discrete( discreteBucketsFor( LNG_ARR, 1, 2 ) ) ),
                        new PropertyDefinition( "dis9", discrete( discreteBucketsFor( FLT_ARR, 1, 2 ) ) ),
                        new PropertyDefinition( "dis10", discrete( discreteBucketsFor( DBL_ARR, 1, 2 ) ) ),
                        new PropertyDefinition( "spatial1", PointGenerator.random( 1, 2, 3, 4, new CRS.Cartesian() ) ),
                        new PropertyDefinition( "spatial2", grid( 1, 2, 3, 4, 5, new CRS.WGS84() ) ),
                        new PropertyDefinition( "spatial3", circleGrid( from( 1, 1, 2, 2, 0, 10, 0, 10, 100, new CRS.Cartesian() ) ) ),
                        new PropertyDefinition( "spatial4", circleGrid( from( 1, 1, 0, 10, 0, 10, 100, new CRS.WGS84() ) ) ),
                        new PropertyDefinition( "spatial5", clusterGrid( from( 1, 1, 2, 2, 0, 10, 0, 10, 100, new CRS.Cartesian() ) ) ),
                        new PropertyDefinition( "spatial6", clusterGrid( from( 1, 1, 0, 10, 0, 10, 100, new CRS.WGS84() ) ) ),
                        new PropertyDefinition( "spatial7", diagonal( randDouble( 1, 2 ), new CRS.Cartesian() ) ),
                        new PropertyDefinition( "temporal1", date( ascLong( 1 ) ) ),
                        new PropertyDefinition( "temporal2", dateTime( ascLong( 2 ) ) ),
                        new PropertyDefinition( "temporal3", duration( ascLong( 3 ) ) ),
                        new PropertyDefinition( "temporal4", localDatetime( ascLong( 4 ) ) ),
                        new PropertyDefinition( "temporal5", localTime( ascLong( 5 ) ) ),
                        new PropertyDefinition( "temporal6", time( ascLong( 6 ) ) )
                )
                .withRelationshipProperties(
                        new PropertyDefinition( "rand1", randPropertyFor( STR_SML ).value() ),
                        new PropertyDefinition( "rand2", randPropertyFor( STR_BIG ).value() ),
                        new PropertyDefinition( "rand3", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "rand4", randPropertyFor( LNG ).value() ),
                        new PropertyDefinition( "rand5", randPropertyFor( FLT ).value() ),
                        new PropertyDefinition( "rand6", randPropertyFor( DBL ).value() ),
                        new PropertyDefinition( "rand7", randPropertyFor( INT_ARR ).value() ),
                        new PropertyDefinition( "rand8", randPropertyFor( LNG_ARR ).value() ),
                        new PropertyDefinition( "rand9", randPropertyFor( FLT_ARR ).value() ),
                        new PropertyDefinition( "rand10", randPropertyFor( DBL_ARR ).value() ),
                        new PropertyDefinition( "asc1", ascPropertyFor( STR_SML ).value() ),
                        new PropertyDefinition( "asc2", ascPropertyFor( STR_BIG ).value() ),
                        new PropertyDefinition( "asc3", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "asc4", ascPropertyFor( LNG ).value() ),
                        new PropertyDefinition( "asc5", ascPropertyFor( FLT ).value() ),
                        new PropertyDefinition( "asc6", ascPropertyFor( DBL ).value() ),
                        new PropertyDefinition( "asc7", ascPropertyFor( INT_ARR ).value() ),
                        new PropertyDefinition( "asc8", ascPropertyFor( LNG_ARR ).value() ),
                        new PropertyDefinition( "asc9", ascPropertyFor( FLT_ARR ).value() ),
                        new PropertyDefinition( "asc10", ascPropertyFor( DBL_ARR ).value() ),
                        new PropertyDefinition( "dis1", discrete( discreteBucketsFor( STR_SML, 1, 2 ) ) ),
                        new PropertyDefinition( "dis2", discrete( discreteBucketsFor( STR_BIG, 1, 2 ) ) ),
                        new PropertyDefinition( "dis3", discrete( discreteBucketsFor( INT, 1, 2 ) ) ),
                        new PropertyDefinition( "dis4", discrete( discreteBucketsFor( LNG, 1, 2 ) ) ),
                        new PropertyDefinition( "dis5", discrete( discreteBucketsFor( FLT, 1, 2 ) ) ),
                        new PropertyDefinition( "dis6", discrete( discreteBucketsFor( DBL, 1, 2 ) ) ),
                        new PropertyDefinition( "dis7", discrete( discreteBucketsFor( INT_ARR, 1, 2 ) ) ),
                        new PropertyDefinition( "dis8", discrete( discreteBucketsFor( LNG_ARR, 1, 2 ) ) ),
                        new PropertyDefinition( "dis9", discrete( discreteBucketsFor( FLT_ARR, 1, 2 ) ) ),
                        new PropertyDefinition( "dis10", discrete( discreteBucketsFor( DBL_ARR, 1, 2 ) ) ),
                        new PropertyDefinition( "spatial1", PointGenerator.random( 1, 2, 3, 4, new CRS.Cartesian() ) ),
                        new PropertyDefinition( "spatial2", grid( 1, 2, 3, 4, 5, new CRS.WGS84() ) ),
                        new PropertyDefinition( "spatial3", circleGrid( from( 1, 1, 2, 2, 0, 10, 0, 10, 100, new CRS.Cartesian() ) ) ),
                        new PropertyDefinition( "spatial4", circleGrid( from( 1, 1, 0, 10, 0, 10, 100, new CRS.WGS84() ) ) ),
                        new PropertyDefinition( "spatial5", clusterGrid( from( 1, 1, 2, 2, 0, 10, 0, 10, 100, new CRS.Cartesian() ) ) ),
                        new PropertyDefinition( "spatial6", clusterGrid( from( 1, 1, 0, 10, 0, 10, 100, new CRS.WGS84() ) ) ),
                        new PropertyDefinition( "spatial7", diagonal( randDouble( 1, 2 ), new CRS.Cartesian() ) ),
                        new PropertyDefinition( "temporal1", date( ascLong( 1 ) ) ),
                        new PropertyDefinition( "temporal2", dateTime( ascLong( 2 ) ) ),
                        new PropertyDefinition( "temporal3", duration( ascLong( 3 ) ) ),
                        new PropertyDefinition( "temporal4", localDatetime( ascLong( 4 ) ) ),
                        new PropertyDefinition( "temporal5", localTime( ascLong( 5 ) ) ),
                        new PropertyDefinition( "temporal6", time( ascLong( 6 ) ) )
                )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void configurationsWithDifferentPropertyTypesShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNodeProperties( new PropertyDefinition( "dis1", discrete( discreteBucketsFor( INT, 1, 2 ) ) ) )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNodeProperties( new PropertyDefinition( "dis1", discrete( discreteBucketsFor( LNG, 1, 2 ) ) ) )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSamePropertyOrderShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withPropertyOrder( Order.ORDERED )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentPropertyOrderShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withPropertyOrder( Order.ORDERED )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withPropertyOrder( Order.SHUFFLED )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSamePropertyLocalityShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withPropertyLocality( PropertyLocality.SCATTERED_BY_ELEMENT )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentPropertyLocalityShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withPropertyLocality( PropertyLocality.SCATTERED_BY_ELEMENT )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withPropertyLocality( PropertyLocality.CO_LOCATED_BY_ELEMENT )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSameLabelsShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withLabels(
                        Label.label( "label1" ),
                        Label.label( "label2" ) )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentLabelsShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withLabels(
                        Label.label( "label1" ),
                        Label.label( "label2" ) )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withLabels(
                        Label.label( "label1" ),
                        Label.label( "label3" ) )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSameLabelOrderShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withLabelOrder( Order.SHUFFLED )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentLabelOrderShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withLabelOrder( Order.SHUFFLED )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withLabelOrder( Order.ORDERED )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSameLabelLocalityShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withLabelLocality( LabelLocality.SCATTERED_BY_NODE )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentLabelLocalityShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withLabelLocality( LabelLocality.SCATTERED_BY_NODE )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withLabelLocality( LabelLocality.CO_LOCATED_BY_NODE )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSameRandomSeedShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withRngSeed( 1 )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentRandomSeedShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withRngSeed( 1 )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withRngSeed( 2 )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSameNeo4jConfigShouldBeEqual() throws IOException
    {
        Neo4jConfig neo4jConfig = new Neo4jConfig( MapUtil.genericMap(
                "a", "1",
                "b", "\"2\"",
                "c", "3",
                "d", "a/b/c/d.e",
                "e", "true" ) );

        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNeo4jConfig( neo4jConfig )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentNeo4jConfigShouldNotBeEqual() throws IOException
    {
        Neo4jConfig neo4jConfig1 = new Neo4jConfig( MapUtil.genericMap(
                "a", "1",
                "b", "\"2\"",
                "c", "3",
                "d", "a/b/c/d.e",
                "e", "true" ) );

        Neo4jConfig neo4jConfig2 = new Neo4jConfig( MapUtil.genericMap(
                "a", "1",
                "b", "\"2\"",
                "c", "3",
                "d", "a/b/c/d.e",
                "e", "false" ) );

        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNeo4jConfig( neo4jConfig1 )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withNeo4jConfig( neo4jConfig2 )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSameSchemaIndexesShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentSchemaIndexesShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "C" ), "b" ) )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSameUniqueConstraintsShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withUniqueConstraints(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentUniqueConstraintsShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withUniqueConstraints(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withUniqueConstraints(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "C" ), "b" ) )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithSameFulltextNodeSchemaIndexesShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withFulltextNodeSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithSameFulltextRelationshipSchemaIndexesShouldBeEqual() throws IOException
    {
        Supplier<DataGeneratorConfig> fun = () -> new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withFulltextRelationshipSchemaIndexes(
                        new RelationshipKeyDefinition( RelationshipType.withName( "A" ), "a" ),
                        new RelationshipKeyDefinition( RelationshipType.withName( "B" ), "b" ) )
                .build();

        DataGeneratorConfig config1 = fun.get();
        DataGeneratorConfig config2 = fun.get();
        assertConfigEquality( config1, config2, true );
    }

    @Test
    public void dataGeneratorsWithDifferentFulltextNodeSchemaIndexesShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withFulltextNodeSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "B" ), "b" ) )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withFulltextNodeSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "A" ), "a" ),
                        new LabelKeyDefinition( Label.label( "C" ), "b" ) )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    @Test
    public void dataGeneratorsWithDifferentFulltextRelationshipSchemaIndexesShouldNotBeEqual() throws IOException
    {
        DataGeneratorConfig config1 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withFulltextRelationshipSchemaIndexes(
                        new RelationshipKeyDefinition( RelationshipType.withName( "A" ), "a" ),
                        new RelationshipKeyDefinition( RelationshipType.withName( "B" ), "b" ) )
                .build();

        DataGeneratorConfig config2 = new DataGeneratorConfigBuilder()
                .isReusableStore( true )
                .withFulltextRelationshipSchemaIndexes(
                        new RelationshipKeyDefinition( RelationshipType.withName( "A" ), "a" ),
                        new RelationshipKeyDefinition( RelationshipType.withName( "C" ), "b" ) )
                .build();

        assertConfigEquality( config1, config2, false );
    }

    private void assertConfigEquality( DataGeneratorConfig config1, DataGeneratorConfig config2, boolean value )
            throws IOException
    {
        // equal to begin with
        assertThat( format( "%s\n%s", config1, config2 ),
                    config1.equals( config2 ), equalTo( value ) );

        File config1File = temporaryFolder.file( "config1.file" );
        config1.serialize( config1File.toPath() );

        File config2File =  temporaryFolder.file( "config2.file" );
        config2.serialize( config2File.toPath() );

        DataGeneratorConfig config1After = DataGeneratorConfig.from( config1File.toPath() );
        DataGeneratorConfig config2After = DataGeneratorConfig.from( config2File.toPath() );

        // configs after serialize and marshall are equal to what they were originally
        assertThat( format( "%s\n%s", config1, config1After ), config1, equalTo( config1After ) );
        assertThat( format( "%s\n%s", config2, config2After ), config2, equalTo( config2After ) );

        // configs after serialize and marshall are equal to each other
        assertThat( format( "%s\n%s", config1After, config2After ),
                    config1After.equals( config2After ), equalTo( value ) );
    }
}
