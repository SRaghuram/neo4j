/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.micro.data.DataGenerator.GraphWriter;
import com.neo4j.bench.micro.data.DataGenerator.LabelLocality;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGenerator.PropertyLocality;
import com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.common.util.TestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.IntStream;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.micro.data.DataGeneratorTestUtil.assertGraphStatsAreConsistentWithBuilderConfiguration;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.ascPropertyFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;
import static org.neo4j.graphdb.Label.label;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class DataGeneratorDataCharacteristicsIT
{
    @Inject
    private TestDirectory temporaryFolder;

    private static final Neo4jConfig NEO4J_CONFIG = Neo4jConfigBuilder.withDefaults().build();

    private static final double TOLERANCE = 0.05;
    private static final long RNG_SEED = 42;
    private static final int NODE_COUNT = 100_000;

    private DataGeneratorConfigBuilder builder;
    private Path neo4jConfigPath;

    @BeforeEach
    void createBuilder()
    {
        builder = new DataGeneratorConfigBuilder()
                .withRngSeed( RNG_SEED );
        Path absoluteTempPath = temporaryFolder.absolutePath();
        neo4jConfigPath = absoluteTempPath.resolve( "neo4j.config" );
        Neo4jConfigBuilder.writeToFile( NEO4J_CONFIG, neo4jConfigPath );
    }

    @Test
    void shouldCreateEmptyGraph() throws IOException
    {
        // Given
        Store store = com.neo4j.common.util.TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( 0 );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateJustNodesBatch() throws IOException
    {
        shouldCreateJustNodes( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateJustNodesTransactional() throws IOException
    {
        shouldCreateJustNodes( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateJustNodes( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldNotCrashWhenConfiguredToCreateJustRelationshipsBatch() throws IOException
    {
        shouldNotCrashWhenConfiguredToCreateJustRelationships( GraphWriter.BATCH );
    }

    @Test
    void shouldNotCrashWhenConfiguredToCreateJustRelationshipsTransactional() throws IOException
    {
        shouldNotCrashWhenConfiguredToCreateJustRelationships( GraphWriter.TRANSACTIONAL );
    }

    private void shouldNotCrashWhenConfiguredToCreateJustRelationships( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "TYPE" ), 10 ) )
                .withRelationshipLocality( RelationshipLocality.CO_LOCATED_BY_START_NODE )
                .withNodeProperties( new PropertyDefinition( "1", randPropertyFor( DBL ).value() ) )
                .withPropertyLocality( PropertyLocality.CO_LOCATED_BY_ELEMENT )
                .withPropertyOrder( Order.ORDERED );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateShuffledNodePropertyChainBatch() throws IOException
    {
        shouldCreateShuffledNodePropertyChain( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateShuffledNodePropertyChainTransactional() throws IOException
    {
        shouldCreateShuffledNodePropertyChain( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateShuffledNodePropertyChain( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        label( "Label" ) )
                .withMandatoryNodeConstraints(
                        new LabelKeyDefinition( label( "Label" ), "a" ),
                        new LabelKeyDefinition( label( "Label" ), "b" ),
                        new LabelKeyDefinition( label( "Label" ), "c" ),
                        new LabelKeyDefinition( label( "Label" ), "d" ),
                        new LabelKeyDefinition( label( "Label" ), "e" ),
                        new LabelKeyDefinition( label( "Label" ), "f" ),
                        new LabelKeyDefinition( label( "Label" ), "g" ),
                        new LabelKeyDefinition( label( "Label" ), "h" ),
                        new LabelKeyDefinition( label( "Label" ), "i" ),
                        new LabelKeyDefinition( label( "Label" ), "j" ) )
                .withSchemaIndexes(
                        new LabelKeyDefinition( label( "Label" ), "a" ),
                        new LabelKeyDefinition( label( "Label" ), "b" ),
                        new LabelKeyDefinition( label( "Label" ), "c" ),
                        new LabelKeyDefinition( label( "Label" ), "d" ),
                        new LabelKeyDefinition( label( "Label" ), "e" ),
                        new LabelKeyDefinition( label( "Label" ), "f" ),
                        new LabelKeyDefinition( label( "Label" ), "g" ),
                        new LabelKeyDefinition( label( "Label" ), "h" ),
                        new LabelKeyDefinition( label( "Label" ), "i" ),
                        new LabelKeyDefinition( label( "Label" ), "j" ) )
                .withNodeProperties(
                        new PropertyDefinition( "a", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "b", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "c", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "d", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "e", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "f", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "g", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "h", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "i", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "j", randPropertyFor( INT ).value() ) )
                .withPropertyLocality( PropertyLocality.CO_LOCATED_BY_ELEMENT )
                .withPropertyOrder( Order.SHUFFLED );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldOrderedNodePropertyChainBatch() throws IOException
    {
        shouldOrderedNodePropertyChain( GraphWriter.BATCH );
    }

    @Test
    void shouldOrderedNodePropertyChainTransactional() throws IOException
    {
        shouldOrderedNodePropertyChain( GraphWriter.TRANSACTIONAL );
    }

    private void shouldOrderedNodePropertyChain( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        label( "Label" ) )
                .withMandatoryNodeConstraints(
                        new LabelKeyDefinition( label( "Label" ), "a" ),
                        new LabelKeyDefinition( label( "Label" ), "b" ),
                        new LabelKeyDefinition( label( "Label" ), "c" ),
                        new LabelKeyDefinition( label( "Label" ), "d" ),
                        new LabelKeyDefinition( label( "Label" ), "e" ),
                        new LabelKeyDefinition( label( "Label" ), "f" ),
                        new LabelKeyDefinition( label( "Label" ), "g" ),
                        new LabelKeyDefinition( label( "Label" ), "h" ),
                        new LabelKeyDefinition( label( "Label" ), "i" ),
                        new LabelKeyDefinition( label( "Label" ), "j" ) )
                .withSchemaIndexes(
                        new LabelKeyDefinition( label( "Label" ), "a" ),
                        new LabelKeyDefinition( label( "Label" ), "b" ),
                        new LabelKeyDefinition( label( "Label" ), "c" ),
                        new LabelKeyDefinition( label( "Label" ), "d" ),
                        new LabelKeyDefinition( label( "Label" ), "e" ),
                        new LabelKeyDefinition( label( "Label" ), "f" ),
                        new LabelKeyDefinition( label( "Label" ), "g" ),
                        new LabelKeyDefinition( label( "Label" ), "h" ),
                        new LabelKeyDefinition( label( "Label" ), "i" ),
                        new LabelKeyDefinition( label( "Label" ), "j" ) )
                .withNodeProperties(
                        new PropertyDefinition( "a", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "b", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "c", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "d", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "e", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "f", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "g", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "h", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "i", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "j", randPropertyFor( INT ).value() ) )
                .withPropertyLocality( PropertyLocality.SCATTERED_BY_ELEMENT )
                .withPropertyOrder( Order.ORDERED );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateUniqueConstrainedAndIndexedNodePropertiesBatch() throws IOException
    {
        shouldCreateUniqueConstrainedAndIndexedNodeProperties( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateUniqueConstrainedAndIndexedNodePropertiesTransactional() throws IOException
    {
        shouldCreateUniqueConstrainedAndIndexedNodeProperties( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateUniqueConstrainedAndIndexedNodeProperties( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        label( "Label" ) )
                .withMandatoryNodeConstraints(
                        new LabelKeyDefinition( label( "Label" ), "a" ),
                        new LabelKeyDefinition( label( "Label" ), "b" ),
                        new LabelKeyDefinition( label( "Label" ), "c" ),
                        new LabelKeyDefinition( label( "Label" ), "d" ),
                        new LabelKeyDefinition( label( "Label" ), "e" ),
                        new LabelKeyDefinition( label( "Label" ), "f" ),
                        new LabelKeyDefinition( label( "Label" ), "g" ),
                        new LabelKeyDefinition( label( "Label" ), "h" ),
                        new LabelKeyDefinition( label( "Label" ), "i" ),
                        new LabelKeyDefinition( label( "Label" ), "j" ) )
                .withUniqueConstraints(
                        new LabelKeyDefinition( label( "Label" ), "a" ),
                        new LabelKeyDefinition( label( "Label" ), "b" ),
                        new LabelKeyDefinition( label( "Label" ), "c" ),
                        new LabelKeyDefinition( label( "Label" ), "d" ),
                        new LabelKeyDefinition( label( "Label" ), "e" ) )
                .withSchemaIndexes(
                        new LabelKeyDefinition( label( "Label" ), "f" ),
                        new LabelKeyDefinition( label( "Label" ), "g" ),
                        new LabelKeyDefinition( label( "Label" ), "h" ),
                        new LabelKeyDefinition( label( "Label" ), "i" ),
                        new LabelKeyDefinition( label( "Label" ), "j" ) )
                .withNodeProperties(
                        new PropertyDefinition( "a", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "b", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "c", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "d", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "e", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "f", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "g", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "h", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "i", ascPropertyFor( INT ).value() ),
                        new PropertyDefinition( "j", ascPropertyFor( INT ).value() ) )
                .withPropertyLocality( PropertyLocality.CO_LOCATED_BY_ELEMENT )
                .withPropertyOrder( Order.SHUFFLED );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateShuffledRelationshipPropertyChainBatch() throws IOException
    {
        shouldCreateShuffledRelationshipPropertyChain( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateShuffledRelationshipPropertyChainTransactional() throws IOException
    {
        shouldCreateShuffledRelationshipPropertyChain( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateShuffledRelationshipPropertyChain( GraphWriter graphWriter ) throws IOException
    {
        // Given

        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "TYPE" ), 10 ) )
                .withRelationshipProperties(
                        new PropertyDefinition( "0", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "1", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "2", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "3", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "4", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "5", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "6", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "7", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "8", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "9", randPropertyFor( INT ).value() ) )
                .withPropertyLocality( PropertyLocality.CO_LOCATED_BY_ELEMENT )
                .withPropertyOrder( Order.SHUFFLED );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateOrderedRelationshipPropertyChainBatch() throws IOException
    {
        shouldCreateOrderedRelationshipPropertyChain( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateOrderedRelationshipPropertyChainTransactional() throws IOException
    {
        shouldCreateOrderedRelationshipPropertyChain( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateOrderedRelationshipPropertyChain( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "TYPE" ), 10 ) )
                .withRelationshipProperties(
                        new PropertyDefinition( "0", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "1", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "2", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "3", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "4", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "5", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "6", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "7", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "8", randPropertyFor( INT ).value() ),
                        new PropertyDefinition( "9", randPropertyFor( INT ).value() ) )
                .withPropertyLocality( PropertyLocality.CO_LOCATED_BY_ELEMENT )
                .withPropertyOrder( Order.ORDERED );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateRelationshipsCollocatedByStartNodeBatch() throws IOException
    {
        shouldCreateRelationshipsCollocatedByStartNode( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateRelationshipsCollocatedByStartNodeTransactional() throws IOException
    {
        shouldCreateRelationshipsCollocatedByStartNode( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateRelationshipsCollocatedByStartNode( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "TYPE" ), 10 ) )
                .withRelationshipLocality( RelationshipLocality.CO_LOCATED_BY_START_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateRelationshipsScatteredByStartNodeBatch() throws IOException
    {
        shouldCreateRelationshipsScatteredByStartNode( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateRelationshipsScatteredByStartNodeTransactional() throws IOException
    {
        shouldCreateRelationshipsScatteredByStartNode( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateRelationshipsScatteredByStartNode( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "TYPE" ), 10 ) )
                .withRelationshipLocality( RelationshipLocality.SCATTERED_BY_START_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateScatteredShuffledLabelsBatch() throws IOException
    {
        shouldCreateScatteredShuffledLabels( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateScatteredShuffledLabelsTransactional() throws IOException
    {
        shouldCreateScatteredShuffledLabels( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateScatteredShuffledLabels( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        label( "Label1" ),
                        label( "Label2" ),
                        label( "Label3" ),
                        label( "Label4" ),
                        label( "Label5" ),
                        label( "Label6" ),
                        label( "Label7" ),
                        label( "Label8" ),
                        label( "Label9" ),
                        label( "Label10" ) )
                .withLabelOrder( Order.SHUFFLED )
                .withLabelLocality( LabelLocality.SCATTERED_BY_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateScatteredOrderedLabelsBatch() throws IOException
    {
        shouldCreateScatteredOrderedLabels( GraphWriter.TRANSACTIONAL );
    }

    @Test
    void shouldCreateScatteredOrderedLabelsTransactional() throws IOException
    {
        shouldCreateScatteredOrderedLabels( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateScatteredOrderedLabels( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        label( "Label1" ),
                        label( "Label2" ),
                        label( "Label3" ),
                        label( "Label4" ),
                        label( "Label5" ),
                        label( "Label6" ),
                        label( "Label7" ),
                        label( "Label8" ),
                        label( "Label9" ),
                        label( "Label10" ) )
                .withLabelOrder( Order.ORDERED )
                .withLabelLocality( LabelLocality.SCATTERED_BY_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateCollocatedOrderedLabelsBatch() throws IOException
    {
        shouldCreateCollocatedOrderedLabels( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateCollocatedOrderedLabelsTransactional() throws IOException
    {
        shouldCreateCollocatedOrderedLabels( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateCollocatedOrderedLabels( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        label( "Label1" ),
                        label( "Label2" ),
                        label( "Label3" ),
                        label( "Label4" ),
                        label( "Label5" ),
                        label( "Label6" ),
                        label( "Label7" ),
                        label( "Label8" ),
                        label( "Label9" ),
                        label( "Label10" ) )
                .withLabelOrder( Order.ORDERED )
                .withLabelLocality( LabelLocality.CO_LOCATED_BY_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateCollocatedShuffledLabelsBatch() throws IOException
    {
        shouldCreateCollocatedShuffledLabels( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateCollocatedShuffledLabelsTransactional() throws IOException
    {
        shouldCreateCollocatedShuffledLabels( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateCollocatedShuffledLabels( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        label( "Label1" ),
                        label( "Label2" ),
                        label( "Label3" ),
                        label( "Label4" ),
                        label( "Label5" ),
                        label( "Label6" ),
                        label( "Label7" ),
                        label( "Label8" ),
                        label( "Label9" ),
                        label( "Label10" ) )
                .withLabelOrder( Order.SHUFFLED )
                .withLabelLocality( LabelLocality.CO_LOCATED_BY_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateScatteredShuffledRelationshipsBatch() throws IOException
    {
        shouldCreateScatteredShuffledRelationships( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateScatteredShuffledRelationshipsTransactional() throws IOException
    {
        shouldCreateScatteredShuffledRelationships( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateScatteredShuffledRelationships( GraphWriter graphWriter ) throws IOException
    {
        // Given

        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withOutRelationships(
                        IntStream.range( 1, 26 ).boxed()
                                 .map( i -> "TYPE_" + i )
                                 .map( RelationshipType::withName )
                                 .map( r -> new RelationshipDefinition( r, 1 ) )
                                 .toArray( RelationshipDefinition[]::new ) )
                .withRelationshipOrder( Order.SHUFFLED )
                .withRelationshipLocality( RelationshipLocality.SCATTERED_BY_START_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }

    @Test
    void shouldCreateScatteredOrderedRelationshipsBatch() throws IOException
    {
        shouldCreateScatteredOrderedRelationships( GraphWriter.BATCH );
    }

    @Test
    void shouldCreateScatteredOrderedRelationshipsTransactional() throws IOException
    {
        shouldCreateScatteredOrderedRelationships( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateScatteredOrderedRelationships( GraphWriter graphWriter ) throws IOException
    {
        // Given
        Store store = TestSupport.createEmptyStore( temporaryFolder.homePath(), neo4jConfigPath );

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withOutRelationships(
                        IntStream.range( 1, 26 ).boxed()
                                 .map( i -> "TYPE_" + i )
                                 .map( RelationshipType::withName )
                                 .map( r -> new RelationshipDefinition( r, 1 ) )
                                 .toArray( RelationshipDefinition[]::new ) )
                .withRelationshipOrder( Order.ORDERED )
                .withRelationshipLocality( RelationshipLocality.SCATTERED_BY_START_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( store, builder, TOLERANCE );
    }
}
