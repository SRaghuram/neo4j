package com.neo4j.bench.micro.data;

import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.micro.data.DataGenerator.GraphWriter;
import com.neo4j.bench.micro.data.DataGenerator.LabelLocality;
import com.neo4j.bench.micro.data.DataGenerator.Order;
import com.neo4j.bench.micro.data.DataGenerator.PropertyLocality;
import com.neo4j.bench.micro.data.DataGenerator.RelationshipLocality;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.stream.IntStream;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import static com.neo4j.bench.micro.data.DataGeneratorTestUtil.assertGraphStatsAreConsistentWithBuilderConfiguration;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.DBL;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.INT;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.ascPropertyFor;
import static com.neo4j.bench.micro.data.ValueGeneratorUtil.randPropertyFor;

public class DataGeneratorDataCharacteristicsIT
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final Neo4jConfig NEO4J_CONFIG = Neo4jConfig.empty();

    private static final double TOLERANCE = 0.05;
    private static final long RNG_SEED = 42;
    private static final int NODE_COUNT = 100_000;

    private DataGeneratorConfigBuilder builder;

    @Before
    public void createBuilder()
    {
        builder = new DataGeneratorConfigBuilder()
                .withRngSeed( RNG_SEED );
    }

    @Test
    public void shouldCreateEmptyGraph() throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( 0 );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateJustNodesBatch() throws IOException
    {
        shouldCreateJustNodes( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateJustNodesTransactional() throws IOException
    {
        shouldCreateJustNodes( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateJustNodes( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldNotCrashWhenConfiguredToCreateJustRelationshipsBatch() throws IOException
    {
        shouldNotCrashWhenConfiguredToCreateJustRelationships( GraphWriter.BATCH );
    }

    @Test
    public void shouldNotCrashWhenConfiguredToCreateJustRelationshipsTransactional() throws IOException
    {
        shouldNotCrashWhenConfiguredToCreateJustRelationships( GraphWriter.TRANSACTIONAL );
    }

    private void shouldNotCrashWhenConfiguredToCreateJustRelationships( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "TYPE" ), 10 ) )
                .withRelationshipLocality( RelationshipLocality.CO_LOCATED_BY_START_NODE )
                .withNodeProperties( new PropertyDefinition( "1", randPropertyFor( DBL ).value() ) )
                .withPropertyLocality( PropertyLocality.CO_LOCATED_BY_ELEMENT )
                .withPropertyOrder( Order.ORDERED );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateShuffledNodePropertyChainBatch() throws IOException
    {
        shouldCreateShuffledNodePropertyChain( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateShuffledNodePropertyChainTransactional() throws IOException
    {
        shouldCreateShuffledNodePropertyChain( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateShuffledNodePropertyChain( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        Label.label( "Label" ) )
                .withMandatoryNodeConstraints(
                        new LabelKeyDefinition( Label.label( "Label" ), "a" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "b" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "c" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "d" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "e" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "f" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "g" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "h" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "i" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "j" ) )
                .withSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "Label" ), "a" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "b" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "c" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "d" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "e" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "f" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "g" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "h" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "i" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "j" ) )
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

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldOrderedNodePropertyChainBatch() throws IOException
    {
        shouldOrderedNodePropertyChain( GraphWriter.BATCH );
    }

    @Test
    public void shouldOrderedNodePropertyChainTransactional() throws IOException
    {
        shouldOrderedNodePropertyChain( GraphWriter.TRANSACTIONAL );
    }

    private void shouldOrderedNodePropertyChain( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        Label.label( "Label" ) )
                .withMandatoryNodeConstraints(
                        new LabelKeyDefinition( Label.label( "Label" ), "a" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "b" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "c" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "d" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "e" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "f" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "g" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "h" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "i" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "j" ) )
                .withSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "Label" ), "a" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "b" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "c" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "d" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "e" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "f" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "g" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "h" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "i" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "j" ) )
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

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateUniqueConstrainedAndIndexedNodePropertiesBatch() throws IOException
    {
        shouldCreateUniqueConstrainedAndIndexedNodeProperties( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateUniqueConstrainedAndIndexedNodePropertiesTransactional() throws IOException
    {
        shouldCreateUniqueConstrainedAndIndexedNodeProperties( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateUniqueConstrainedAndIndexedNodeProperties( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        Label.label( "Label" ) )
                .withMandatoryNodeConstraints(
                        new LabelKeyDefinition( Label.label( "Label" ), "a" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "b" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "c" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "d" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "e" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "f" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "g" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "h" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "i" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "j" ) )
                .withUniqueConstraints(
                        new LabelKeyDefinition( Label.label( "Label" ), "a" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "b" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "c" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "d" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "e" ) )
                .withSchemaIndexes(
                        new LabelKeyDefinition( Label.label( "Label" ), "f" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "g" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "h" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "i" ),
                        new LabelKeyDefinition( Label.label( "Label" ), "j" ) )
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

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateShuffledRelationshipPropertyChainBatch() throws IOException
    {
        shouldCreateShuffledRelationshipPropertyChain( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateShuffledRelationshipPropertyChainTransactional() throws IOException
    {
        shouldCreateShuffledRelationshipPropertyChain( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateShuffledRelationshipPropertyChain( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

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

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateOrderedRelationshipPropertyChainBatch() throws IOException
    {
        shouldCreateOrderedRelationshipPropertyChain( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateOrderedRelationshipPropertyChainTransactional() throws IOException
    {
        shouldCreateOrderedRelationshipPropertyChain( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateOrderedRelationshipPropertyChain( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

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

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateRelationshipsCollocatedByStartNodeBatch() throws IOException
    {
        shouldCreateRelationshipsCollocatedByStartNode( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateRelationshipsCollocatedByStartNodeTransactional() throws IOException
    {
        shouldCreateRelationshipsCollocatedByStartNode( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateRelationshipsCollocatedByStartNode( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "TYPE" ), 10 ) )
                .withRelationshipLocality( RelationshipLocality.CO_LOCATED_BY_START_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateRelationshipsScatteredByStartNodeBatch() throws IOException
    {
        shouldCreateRelationshipsScatteredByStartNode( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateRelationshipsScatteredByStartNodeTransactional() throws IOException
    {
        shouldCreateRelationshipsScatteredByStartNode( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateRelationshipsScatteredByStartNode( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withOutRelationships( new RelationshipDefinition( RelationshipType.withName( "TYPE" ), 10 ) )
                .withRelationshipLocality( RelationshipLocality.SCATTERED_BY_START_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateScatteredShuffledLabelsBatch() throws IOException
    {
        shouldCreateScatteredShuffledLabels( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateScatteredShuffledLabelsTransactional() throws IOException
    {
        shouldCreateScatteredShuffledLabels( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateScatteredShuffledLabels( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        Label.label( "Label1" ),
                        Label.label( "Label2" ),
                        Label.label( "Label3" ),
                        Label.label( "Label4" ),
                        Label.label( "Label5" ),
                        Label.label( "Label6" ),
                        Label.label( "Label7" ),
                        Label.label( "Label8" ),
                        Label.label( "Label9" ),
                        Label.label( "Label10" ) )
                .withLabelOrder( Order.SHUFFLED )
                .withLabelLocality( LabelLocality.SCATTERED_BY_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateScatteredOrderedLabelsBatch() throws IOException
    {
        shouldCreateScatteredOrderedLabels( GraphWriter.TRANSACTIONAL );
    }

    @Test
    public void shouldCreateScatteredOrderedLabelsTransactional() throws IOException
    {
        shouldCreateScatteredOrderedLabels( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateScatteredOrderedLabels( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        Label.label( "Label1" ),
                        Label.label( "Label2" ),
                        Label.label( "Label3" ),
                        Label.label( "Label4" ),
                        Label.label( "Label5" ),
                        Label.label( "Label6" ),
                        Label.label( "Label7" ),
                        Label.label( "Label8" ),
                        Label.label( "Label9" ),
                        Label.label( "Label10" ) )
                .withLabelOrder( Order.ORDERED )
                .withLabelLocality( LabelLocality.SCATTERED_BY_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateCollocatedOrderedLabelsBatch() throws IOException
    {
        shouldCreateCollocatedOrderedLabels( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateCollocatedOrderedLabelsTransactional() throws IOException
    {
        shouldCreateCollocatedOrderedLabels( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateCollocatedOrderedLabels( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        Label.label( "Label1" ),
                        Label.label( "Label2" ),
                        Label.label( "Label3" ),
                        Label.label( "Label4" ),
                        Label.label( "Label5" ),
                        Label.label( "Label6" ),
                        Label.label( "Label7" ),
                        Label.label( "Label8" ),
                        Label.label( "Label9" ),
                        Label.label( "Label10" ) )
                .withLabelOrder( Order.ORDERED )
                .withLabelLocality( LabelLocality.CO_LOCATED_BY_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateCollocatedShuffledLabelsBatch() throws IOException
    {
        shouldCreateCollocatedShuffledLabels( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateCollocatedShuffledLabelsTransactional() throws IOException
    {
        shouldCreateCollocatedShuffledLabels( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateCollocatedShuffledLabels( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

        builder
                .withGraphWriter( graphWriter )
                .withNeo4jConfig( NEO4J_CONFIG )
                .withNodeCount( NODE_COUNT )
                .withLabels(
                        Label.label( "Label1" ),
                        Label.label( "Label2" ),
                        Label.label( "Label3" ),
                        Label.label( "Label4" ),
                        Label.label( "Label5" ),
                        Label.label( "Label6" ),
                        Label.label( "Label7" ),
                        Label.label( "Label8" ),
                        Label.label( "Label9" ),
                        Label.label( "Label10" ) )
                .withLabelOrder( Order.SHUFFLED )
                .withLabelLocality( LabelLocality.CO_LOCATED_BY_NODE );

        // Then

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateScatteredShuffledRelationshipsBatch() throws IOException
    {
        shouldCreateScatteredShuffledRelationships( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateScatteredShuffledRelationshipsTransactional() throws IOException
    {
        shouldCreateScatteredShuffledRelationships( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateScatteredShuffledRelationships( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

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

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }

    @Test
    public void shouldCreateScatteredOrderedRelationshipsBatch() throws IOException
    {
        shouldCreateScatteredOrderedRelationships( GraphWriter.BATCH );
    }

    @Test
    public void shouldCreateScatteredOrderedRelationshipsTransactional() throws IOException
    {
        shouldCreateScatteredOrderedRelationships( GraphWriter.TRANSACTIONAL );
    }

    private void shouldCreateScatteredOrderedRelationships( GraphWriter graphWriter ) throws IOException
    {
        // Given

        File storeDir = temporaryFolder.newFolder();

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

        assertGraphStatsAreConsistentWithBuilderConfiguration( storeDir, builder, TOLERANCE );
    }
}
