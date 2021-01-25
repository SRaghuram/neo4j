/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.micro.benchmarks.RNGState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseLayout;

import static com.neo4j.bench.common.util.BenchmarkUtil.durationToString;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;

public class DataGenerator
{
    private static final Logger LOG = LoggerFactory.getLogger( DataGenerator.class );

    public enum PropertyLocality
    {
        // all properties for an element (node/relationship) are NOT written at the same time, e.g.:
        //      (node_1) ADD property_1
        //      (node_2) ADD property_1
        //      (node_3) ADD property_1
        //      (node_1) ADD property_2
        //      (node_2) ADD property_2
        //      (node_3) ADD property_2
        //      (node_1) ADD property_3
        //      (node_2) ADD property_3
        //      (node_3) ADD property_3
        SCATTERED_BY_ELEMENT,
        // all properties for an element (node/relationship) are written at the same time, e.g.:
        //      (node_1) ADD property_1
        //      (node_1) ADD property_2
        //      (node_1) ADD property_3
        //      (node_2) ADD property_1
        //      (node_2) ADD property_2
        //      (node_2) ADD property_3
        //      (node_3) ADD property_1
        //      (node_3) ADD property_2
        //      (node_3) ADD property_3
        CO_LOCATED_BY_ELEMENT
    }

    public enum LabelLocality
    {
        // all labels for an element (node/relationship) are NOT written at the same time, e.g.:
        //      (node_1) ADD Label1
        //      (node_2) ADD Label1
        //      (node_3) ADD Label1
        //      (node_1) ADD Label2
        //      (node_2) ADD Label2
        //      (node_3) ADD Label2
        //      (node_1) ADD Label3
        //      (node_2) ADD Label3
        //      (node_3) ADD Label3
        SCATTERED_BY_NODE,
        // all labels for an element (node/relationship) are written at the same time, e.g.:
        //      (node_1) ADD Label1
        //      (node_1) ADD Label2
        //      (node_1) ADD Label3
        //      (node_2) ADD Label1
        //      (node_2) ADD Label2
        //      (node_2) ADD Label3
        //      (node_3) ADD Label1
        //      (node_3) ADD Label2
        //      (node_3) ADD Label3
        CO_LOCATED_BY_NODE
    }

    public enum RelationshipLocality
    {
        // relationships with same start node are deterministically scattered, e.g.:
        //      (node_1)-[:TYPE]->(node_2)
        //      (node_2)-[:TYPE]->(node_3)
        //      (node_3)-[:TYPE]->(node_4)
        //      (node_1)-[:TYPE]->(node_3)
        //      (node_2)-[:TYPE]->(node_4)
        //      (node_3)-[:TYPE]->(node_5)
        //      (node_1)-[:TYPE]->(node_4)
        SCATTERED_BY_START_NODE,
        // relationships with same start node are deterministically scattered, e.g.:
        //      (node_1)-[:TYPE]->(node_2)
        //      (node_1)-[:TYPE]->(node_3)
        //      (node_1)-[:TYPE]->(node_4)
        //      (node_2)-[:TYPE]->(node_3)
        //      (node_2)-[:TYPE]->(node_4)
        //      (node_2)-[:TYPE]->(node_5)
        //      (node_3)-[:TYPE]->(node_4)
        CO_LOCATED_BY_START_NODE
        // TODO CO_LOCATED_BY_END_NODE
        // TODO SCATTERED_BY_END_NODE
    }

    public enum Order
    {
        // properties are written to nodes in the order specified. same order for every node/relationship.
        ORDERED,
        // properties are written to nodes in shuffled order. different order for every node/relationship.
        SHUFFLED
    }

    public enum GraphWriter
    {
        // all store generation operations are performed transactionally
        TRANSACTIONAL,
        // some store generation operations may be performed using batch inserter
        BATCH
    }

    public static final long DEFAULT_RNG_SEED = 42;
    private static final String NODE_PROPERTY_INDEX_FILENAME_PREFIX = "node.property.indexes.";
    private static final String RELATIONSHIP_PROPERTY_INDEX_FILENAME_PREFIX = "relationship.property.indexes.";
    private static final String RELATIONSHIP_TYPE_INDEX_FILENAME_PREFIX = "relationship.type.indexes.";
    private static final String NODE_LABEL_INDEX_FILENAME_PREFIX = "node.label.indexes.";
    private static final String RELATIONSHIP_ID_FILENAME = "relationship.id";
    private static final int TX_SIZE = 10_000;

    private final Random shuffleRng;
    private final SplittableRandom rng;
    private final int nodes;
    private final RelationshipType[] outRelationshipTypes;
    private final Order relationshipOrder;
    private final RelationshipLocality relationshipLocality;
    private final GraphWriter graphWriter;
    private final String[] nodePropertyKeys;
    private final ValueGeneratorFun[] nodePropertyValues;
    private final String[] relationshipPropertyKeys;
    private final ValueGeneratorFun[] relationshipPropertyValues;
    private final PropertyLocality propertyLocality;
    private final Order propertyOrder;
    private final Label[] labels;
    private final Order labelOrder;
    private final LabelLocality labelLocality;
    private final LabelKeyDefinition[] schemaIndexes;
    private final LabelKeyDefinition[] uniqueConstraints;
    private final LabelKeyDefinition[] mandatoryNodeConstraints;
    private final RelationshipKeyDefinition[] mandatoryRelationshipConstraints;
    private final LabelKeyDefinition[] fulltextNodeSchemaIndexes;
    private final RelationshipKeyDefinition[] fulltextRelationshipSchemaIndexes;

    public DataGenerator( DataGeneratorConfig config )
    {
        this.shuffleRng = new Random( config.rngSeed() );
        this.rng = RNGState.newRandom( config.rngSeed() );
        this.nodes = config.nodeCount();
        this.outRelationshipTypes = toRelationshipTypes( config.outRelationships() );
        this.relationshipOrder = config.relationshipOrder();
        this.relationshipLocality = config.relationshipLocality();
        this.graphWriter = config.graphWriter();
        this.nodePropertyKeys = Stream.of( config.nodeProperties() )
                                      .map( PropertyDefinition::key )
                                      .toArray( String[]::new );
        this.nodePropertyValues = Stream.of( config.nodeProperties() )
                                        .map( PropertyDefinition::value )
                                        .map( ValueGeneratorFactory::create )
                                        .toArray( ValueGeneratorFun[]::new );
        this.relationshipPropertyKeys = Stream.of( config.relationshipProperties() )
                                              .map( PropertyDefinition::key )
                                              .toArray( String[]::new );
        this.relationshipPropertyValues = Stream.of( config.relationshipProperties() )
                                                .map( PropertyDefinition::value )
                                                .map( ValueGeneratorFactory::create )
                                                .toArray( ValueGeneratorFun[]::new );
        this.propertyLocality = config.propertyLocality();
        this.propertyOrder = config.propertyOrder();
        this.labels = config.labels();
        this.labelOrder = config.labelOrder();
        this.labelLocality = config.labelLocality();
        this.schemaIndexes = config.schemaIndexes();
        this.uniqueConstraints = config.uniqueConstraints();
        this.mandatoryNodeConstraints = config.mandatoryNodeConstraints();
        this.mandatoryRelationshipConstraints = config.mandatoryRelationshipConstraints();
        this.fulltextNodeSchemaIndexes = config.fulltextNodeSchemaIndexes();
        this.fulltextRelationshipSchemaIndexes = config.fulltextRelationshipSchemaIndexes();
    }

    private RelationshipType[] toRelationshipTypes( RelationshipDefinition[] relationships )
    {
        int outDegree = Stream.of( relationships ).mapToInt( RelationshipDefinition::count ).sum();
        RelationshipType[] relationshipTypes = new RelationshipType[outDegree];
        int typeIndex = 0;
        for ( RelationshipDefinition relationship : relationships )
        {
            for ( int typeCount = 0; typeCount < relationship.count(); typeCount++ )
            {
                relationshipTypes[typeIndex++] = relationship.type();
            }
        }
        return relationshipTypes;
    }

    private int relationshipCount()
    {
        return nodes * outRelationshipTypes.length;
    }

    void generate( Store store, Path neo4jConfig )
    {
        Instant startTime = Instant.now();
        try
        {
            Path tempOutputDir = Files.createTempDirectory( store.topLevelDirectory(), "temp_data_generator_files" );
            try
            {
                switch ( graphWriter )
                {
                case TRANSACTIONAL:
                    innerTransactionalLastPhase(
                            store,
                            neo4jConfig,
                            innerTransactionalFirstPhase( store, neo4jConfig, tempOutputDir ),
                            tempOutputDir );
                    break;
                case BATCH:
                    innerTransactionalLastPhase(
                            store,
                            neo4jConfig,
                            innerBatchFirstPhase( store, neo4jConfig, tempOutputDir ),
                            tempOutputDir );
                    break;
                default:
                    throw new RuntimeException( "Unrecognized graph writer: " + graphWriter );
                }
            }
            finally
            {
                BenchmarkUtil.deleteDir( tempOutputDir );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "failed to generate data", e );
        }
        Instant finishTime = Instant.now();
        LOG.debug( "Generated store in: " + durationToString( Duration.between( startTime, finishTime ) ) );
    }

    private long[] innerBatchFirstPhase( Store store, Path neo4jConfig, Path tempOutputDir )
    {
        BatchInserter inserter = null;
        try
        {
            Instant startTime = Instant.now();

            Map<String,String> neo4jConfigMap = Neo4jConfigBuilder.fromFile( neo4jConfig ).build().toMap();
            Config config = Config.newBuilder()
                                  .setRaw( neo4jConfigMap )
                                  .set( GraphDatabaseSettings.neo4j_home, store.topLevelDirectory() )
                                  .set( GraphDatabaseSettings.default_database, store.databaseName().name() )
                                  .build();
            inserter = BatchInserters.inserter( DatabaseLayout.of( config ), config );

            LOG.debug( "Creating Nodes... " );
            // NOTE: for node identifiers, use array instead of file, because random access is needed
            long[] nodeIds = createNodesBatch( inserter );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            startTime = Instant.now();
            LOG.debug( "Creating Relationships... " );
            IntFileReader[] relationshipTypeIndexes = Stream
                    .of( createRelationshipTypeIndexFiles( tempOutputDir ) )
                    .map( IntFileReader::new )
                    .toArray( IntFileReader[]::new );
            IntFileReader relationshipIds = createRelationshipsBatch( inserter, nodeIds, relationshipTypeIndexes, tempOutputDir );
            deleteIntFileReaderFiles( relationshipTypeIndexes );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Temporary Node Property Files... " );
            startTime = Instant.now();
            IntFileReader[] nodePropertyIndexes = Stream
                    .of( createNodePropertyIndexFiles( tempOutputDir ) )
                    .map( IntFileReader::new )
                    .toArray( IntFileReader[]::new );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Node Properties... " );
            startTime = Instant.now();
            createNodePropertiesBatch( inserter, nodeIds, nodePropertyIndexes );
            deleteIntFileReaderFiles( nodePropertyIndexes );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Temporary Relationship Property Files... " );
            startTime = Instant.now();
            IntFileReader[] relationshipPropertyIndexes = Stream
                    .of( createRelationshipPropertyIndexFiles( tempOutputDir ) )
                    .map( IntFileReader::new )
                    .toArray( IntFileReader[]::new );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Relationship Properties... " );
            startTime = Instant.now();
            createRelationshipPropertiesBatch( inserter, relationshipIds, relationshipPropertyIndexes );
            deleteIntFileReaderFiles( relationshipPropertyIndexes );
            deleteIntFileReaderFile( relationshipIds );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            return nodeIds;
        }
        catch ( Exception e )
        {
            // print stack trace too, in case JMH swallows it
            e.printStackTrace();
            throw new RuntimeException( "Error generating data with batch inserter", e );
        }
        finally
        {
            if ( null != inserter )
            {
                inserter.shutdown();
            }
        }
    }

    private long[] innerTransactionalFirstPhase( Store store, Path neo4jConfig, Path tempOutputDir )
    {
        GraphDatabaseService db = null;
        try
        {
            Instant startTime = Instant.now();

            db = ManagedStore.newDb( store, neo4jConfig );

            LOG.debug( "Creating Nodes... " );
            // NOTE: for node identifiers, use array instead of file, because random access is needed
            long[] nodeIds = createNodesTx( db );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            startTime = Instant.now();
            LOG.debug( "Creating Relationships... " );
            IntFileReader[] relationshipTypeIndexes = Stream
                    .of( createRelationshipTypeIndexFiles( tempOutputDir ) )
                    .map( IntFileReader::new )
                    .toArray( IntFileReader[]::new );
            IntFileReader relationshipIds = createRelationshipsTx( db, nodeIds, relationshipTypeIndexes, tempOutputDir );
            deleteIntFileReaderFiles( relationshipTypeIndexes );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Temporary Node Property Files... " );
            startTime = Instant.now();
            IntFileReader[] nodePropertyIndexes = Stream
                    .of( createNodePropertyIndexFiles( tempOutputDir ) )
                    .map( IntFileReader::new )
                    .toArray( IntFileReader[]::new );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Node Properties... " );
            startTime = Instant.now();
            createNodePropertiesTx( db, nodeIds, nodePropertyIndexes );
            deleteIntFileReaderFiles( nodePropertyIndexes );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Temporary Relationship Property Files... " );
            startTime = Instant.now();
            IntFileReader[] relationshipPropertyIndexes = Stream
                    .of( createRelationshipPropertyIndexFiles( tempOutputDir ) )
                    .map( IntFileReader::new )
                    .toArray( IntFileReader[]::new );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Relationship Properties... " );
            startTime = Instant.now();
            createRelationshipPropertiesTx( db, relationshipIds, relationshipPropertyIndexes );
            deleteIntFileReaderFiles( relationshipPropertyIndexes );
            deleteIntFileReaderFile( relationshipIds );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            return nodeIds;
        }
        catch ( Exception e )
        {
            // print stack trace too, in case JMH swallows it
            e.printStackTrace();
            throw new RuntimeException( "Error generating data transactionally", e );
        }
        finally
        {
            if ( null != db )
            {
                ManagedStore.getManagementService().shutdown();
            }
        }
    }

    private void innerTransactionalLastPhase( Store store, Path neo4jConfig, long[] nodeIds, Path tempOutputDir )
    {
        GraphDatabaseService db = null;
        try
        {
            db = ManagedStore.newDb( store, neo4jConfig );

            LOG.debug( "Creating Temporary Node Label Files... " );
            Instant startTime = Instant.now();
            IntFileReader[] nodeLabelIndexes = Stream
                    .of( createNodeLabelIndexFiles( tempOutputDir ) )
                    .map( IntFileReader::new )
                    .toArray( IntFileReader[]::new );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Node Labels... " );
            startTime = Instant.now();
            createNodeLabels( db, nodeIds, nodeLabelIndexes );
            deleteIntFileReaderFiles( nodeLabelIndexes );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Mandatory Node Constraints... " );
            startTime = Instant.now();
            createMandatoryNodeConstraints( db );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Mandatory Relationship Constraints... " );
            startTime = Instant.now();
            createMandatoryRelationshipConstraints( db );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Uniqueness Constraints... " );
            startTime = Instant.now();
            createUniquenessConstraints( db );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Creating Schema Indexes... " );
            startTime = Instant.now();
            createSchemaIndexes( db );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );

            LOG.debug( "Waiting For Indexes... " );
            startTime = Instant.now();
            waitForSchemaIndexes( db );
            LOG.debug( durationToString( Duration.between( startTime, Instant.now() ) ) );
        }
        catch ( Exception e )
        {
            // print stack trace too, in case JMH swallows it
            e.printStackTrace();
            throw new RuntimeException( "Error generating data transactionally", e );
        }
        finally
        {
            if ( null != db )
            {
                ManagedStore.getManagementService().shutdown();
            }
        }
    }

    private void deleteIntFileReaderFiles( IntFileReader[] readers ) throws IOException
    {
        for ( IntFileReader reader : readers )
        {
            deleteIntFileReaderFile( reader );
        }
    }

    private void deleteIntFileReaderFile( IntFileReader reader ) throws IOException
    {
        if ( Files.exists( reader.path() ) )
        {
            FileUtils.deleteFile( reader.path() );
        }
    }

    private long[] createNodesTx( GraphDatabaseService db )
    {
        int txStateCounter = 0;
        Transaction tx = db.beginTx();
        long[] nodeIds = new long[nodes];
        try
        {
            for ( int n = 0; n < nodes; n++ )
            {
                long nodeId = tx.createNode().getId();
                nodeIds[n] = nodeId;
                if ( ++txStateCounter % TX_SIZE == 0 )
                {
                    tx.commit();
                    tx.close();
                    tx = db.beginTx();
                }
            }
            return nodeIds;
        }
        finally
        {
            tx.commit();
            tx.close();
        }
    }

    private long[] createNodesBatch( BatchInserter inserter )
    {
        long[] nodeIds = new long[nodes];
        for ( int n = 0; n < nodes; n++ )
        {
            long nodeId = inserter.createNode( emptyMap() );
            nodeIds[n] = nodeId;
        }
        return nodeIds;
    }

    private IntFileReader createRelationshipsTx(
            GraphDatabaseService db,
            long[] nodeIds,
            IntFileReader[] relationshipTypeIndexReaders,
            Path tempOutputDir ) throws Exception
    {
        switch ( relationshipLocality )
        {
        case SCATTERED_BY_START_NODE:
            return createRelationshipsScatteredByStartNodeTx( db, nodeIds, relationshipTypeIndexReaders, tempOutputDir );
        case CO_LOCATED_BY_START_NODE:
            return createRelationshipsCollocatedByStartNodeTx( db, nodeIds, relationshipTypeIndexReaders, tempOutputDir );
        default:
            throw new IllegalArgumentException( format( "Unexpected relationship locality: %s\nExpected one of: %s",
                                                        relationshipLocality.name(),
                                                        Arrays.toString( PropertyLocality.values() ) ) );
        }
    }

    private IntFileReader createRelationshipsBatch(
            BatchInserter inserter,
            long[] nodeIds,
            IntFileReader[] relationshipTypeIndexReaders,
            Path tempOutputDir ) throws Exception
    {
        switch ( relationshipLocality )
        {
        case SCATTERED_BY_START_NODE:
            return createRelationshipsScatteredByStartNodeBatch( inserter, nodeIds, relationshipTypeIndexReaders, tempOutputDir );
        case CO_LOCATED_BY_START_NODE:
            return createRelationshipsCollocatedByStartNodeBatch( inserter, nodeIds, relationshipTypeIndexReaders, tempOutputDir );
        default:
            throw new IllegalArgumentException( format( "Unexpected relationship locality: %s\nExpected one of: %s",
                                                        relationshipLocality.name(),
                                                        Arrays.toString( PropertyLocality.values() ) ) );
        }
    }

    private IntFileReader createRelationshipsScatteredByStartNodeTx(
            GraphDatabaseService db,
            long[] nodeIds,
            IntFileReader[] relationshipTypeIndexReaders,
            Path tempOutputDir ) throws Exception
    {
        int txStateCounter = 0;
        Transaction tx = db.beginTx();
        try ( IntFileWriter relationshipIdsWriter = new IntFileWriter( createRelationshipIdsFile( tempOutputDir ) ) )
        {
            // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
            for ( int position = 0; position < relationshipTypeIndexReaders.length; position++ )
            {
                IntFileReader relationshipTypeIndexReader = relationshipTypeIndexReaders[position];
                for ( int n = 0; n < nodes; n++ )
                {
                    long startNodeId = nodeIds[n];
                    // "position + 1" to avoid self referencing (cycle) relationships at "position == 0"
                    long endNodeId = nodeIds[(n + position + 1) % nodes];
                    IntFileReader.assertAdvance( relationshipTypeIndexReader );
                    RelationshipType relationshipType = outRelationshipTypes[relationshipTypeIndexReader.getInt()];
                    Node startNode = tx.getNodeById( startNodeId );
                    Node endNode = tx.getNodeById( endNodeId );
                    Relationship relationship = startNode.createRelationshipTo( endNode, relationshipType );
                    relationshipIdsWriter.write( (int) relationship.getId() );
                    if ( ++txStateCounter % TX_SIZE == 0 )
                    {
                        tx.commit();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
            return new IntFileReader( relationshipIdsWriter.path() );
        }
        finally
        {
            tx.commit();
            tx.close();
        }
    }

    private IntFileReader createRelationshipsScatteredByStartNodeBatch(
            BatchInserter inserter,
            long[] nodeIds,
            IntFileReader[] relationshipTypeIndexReaders,
            Path tempOutputDir ) throws Exception
    {
        try ( IntFileWriter relationshipIdsWriter = new IntFileWriter( createRelationshipIdsFile( tempOutputDir ) ) )
        {
            // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
            for ( int position = 0; position < relationshipTypeIndexReaders.length; position++ )
            {
                IntFileReader relationshipTypeIndexReader = relationshipTypeIndexReaders[position];
                for ( int n = 0; n < nodes; n++ )
                {
                    long startNodeId = nodeIds[n];
                    // "position + 1" to avoid self referencing (cycle) relationships at "position == 0"
                    long endNodeId = nodeIds[(n + position + 1) % nodes];
                    IntFileReader.assertAdvance( relationshipTypeIndexReader );
                    RelationshipType relationshipType = outRelationshipTypes[relationshipTypeIndexReader.getInt()];
                    long relationshipId = inserter.createRelationship(
                            startNodeId,
                            endNodeId,
                            relationshipType,
                            emptyMap() );
                    relationshipIdsWriter.write( (int) relationshipId );
                }
            }
            return new IntFileReader( relationshipIdsWriter.path() );
        }
    }

    private IntFileReader createRelationshipsCollocatedByStartNodeTx(
            GraphDatabaseService db,
            long[] nodeIds,
            IntFileReader[] relationshipTypeIndexReaders,
            Path tempOutputDir ) throws Exception
    {
        int txStateCounter = 0;
        Transaction tx = db.beginTx();
        try ( IntFileWriter relationshipIdsWriter = new IntFileWriter( createRelationshipIdsFile( tempOutputDir ) ) )
        {
            for ( int n = 0; n < nodes; n++ )
            {
                // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
                for ( int position = 0; position < relationshipTypeIndexReaders.length; position++ )
                {
                    IntFileReader relationshipTypeIndexReader = relationshipTypeIndexReaders[position];
                    long startNodeId = nodeIds[n];
                    // "position + 1" to avoid self referencing (cycle) relationships at "position == 0"
                    long endNodeId = nodeIds[(n + position + 1) % nodes];
                    IntFileReader.assertAdvance( relationshipTypeIndexReader );
                    RelationshipType relationshipType = outRelationshipTypes[relationshipTypeIndexReader.getInt()];
                    Node startNode = tx.getNodeById( startNodeId );
                    Node endNode = tx.getNodeById( endNodeId );
                    Relationship relationship = startNode.createRelationshipTo( endNode, relationshipType );
                    relationshipIdsWriter.write( (int) relationship.getId() );
                    if ( ++txStateCounter % TX_SIZE == 0 )
                    {
                        tx.commit();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
            return new IntFileReader( relationshipIdsWriter.path() );
        }
        finally
        {
            tx.commit();
            tx.close();
        }
    }

    private IntFileReader createRelationshipsCollocatedByStartNodeBatch(
            BatchInserter inserter,
            long[] nodeIds,
            IntFileReader[] relationshipTypeIndexReaders,
            Path tempOutputDir ) throws Exception
    {
        try ( IntFileWriter relationshipIdsWriter = new IntFileWriter( createRelationshipIdsFile( tempOutputDir ) ) )
        {
            for ( int n = 0; n < nodes; n++ )
            {
                // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
                for ( int position = 0; position < relationshipTypeIndexReaders.length; position++ )
                {
                    IntFileReader relationshipTypeIndexReader = relationshipTypeIndexReaders[position];
                    long startNodeId = nodeIds[n];
                    // "position + 1" to avoid self referencing (cycle) relationships at "position == 0"
                    long endNodeId = nodeIds[(n + position + 1) % nodes];
                    IntFileReader.assertAdvance( relationshipTypeIndexReader );
                    RelationshipType relationshipType = outRelationshipTypes[relationshipTypeIndexReader.getInt()];
                    long relationshipId = inserter.createRelationship(
                            startNodeId,
                            endNodeId,
                            relationshipType,
                            emptyMap() );
                    relationshipIdsWriter.write( (int) relationshipId );
                }
            }
            return new IntFileReader( relationshipIdsWriter.path() );
        }
    }

    private Path createRelationshipIdsFile( Path tempOutputDir ) throws IOException
    {
        Path relationshipIdsFile = tempOutputDir.resolve( RELATIONSHIP_ID_FILENAME );
        if ( !relationshipIdsFile.toFile().createNewFile() )
        {
            throw new RuntimeException( "Unable to create file: " + relationshipIdsFile.toAbsolutePath() );
        }
        return relationshipIdsFile;
    }

    private Path[] createNodePropertyIndexFiles( Path tempOutputDir ) throws Exception
    {
        return createArrayIndexFiles(
                nodePropertyValues.length,
                NODE_PROPERTY_INDEX_FILENAME_PREFIX,
                nodes,
                propertyOrder,
                tempOutputDir );
    }

    private Path[] createRelationshipTypeIndexFiles( Path tempOutputDir ) throws Exception
    {
        return createArrayIndexFiles(
                outRelationshipTypes.length,
                RELATIONSHIP_TYPE_INDEX_FILENAME_PREFIX,
                relationshipCount(),
                relationshipOrder,
                tempOutputDir  );
    }

    private Path[] createRelationshipPropertyIndexFiles( Path tempOutputDir ) throws Exception
    {
        return createArrayIndexFiles(
                relationshipPropertyValues.length,
                RELATIONSHIP_PROPERTY_INDEX_FILENAME_PREFIX,
                relationshipCount(),
                propertyOrder,
                tempOutputDir );
    }

    private Path[] createNodeLabelIndexFiles( Path tempOutputDir ) throws Exception
    {
        return createArrayIndexFiles(
                labels.length,
                NODE_LABEL_INDEX_FILENAME_PREFIX,
                nodes,
                labelOrder,
                tempOutputDir );
    }

    private Path[] createArrayIndexFiles( int arrayLength, String pathPrefix, int lines, Order order, Path tempOutputDir ) throws Exception
    {
        final Integer[] indexes = IntStream.range( 0, arrayLength ).boxed().toArray( Integer[]::new );
        final List<Integer> indexesList = Arrays.asList( indexes );
        IntFileWriter[] intFileWriters = new IntFileWriter[indexes.length];

        for ( int i = 0; i < indexes.length; i++ )
        {
            Path path = tempOutputDir.resolve( pathPrefix + i );
            BenchmarkUtil.forceRecreateFile( path );
            intFileWriters[i] = new IntFileWriter( path );
        }

        for ( int line = 1; line <= lines; line++ )
        {
            if ( order == Order.SHUFFLED )
            {
                Collections.shuffle( indexesList, shuffleRng );
            }

            for ( int i = 0; i < indexes.length; i++ )
            {
                intFileWriters[i].write( indexes[i] );
            }
        }

        for ( int i = 0; i < indexes.length; i++ )
        {
            intFileWriters[i].close();
        }

        return Stream.of( intFileWriters ).map( IntFileWriter::path ).toArray( Path[]::new );
    }

    private void createNodePropertiesTx(
            GraphDatabaseService db,
            long[] nodeIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        switch ( propertyLocality )
        {
        case SCATTERED_BY_ELEMENT:
            createNodePropertiesScatteredTx( db, nodeIds, propertyIndexReaders );
            break;
        case CO_LOCATED_BY_ELEMENT:
            createNodePropertiesSequentiallyTx( db, nodeIds, propertyIndexReaders );
            break;
        default:
            throw new IllegalArgumentException( format( "Unexpected property locality: %s\nExpected one of: %s",
                                                        propertyLocality.name(),
                                                        Arrays.toString( PropertyLocality.values() ) ) );
        }
    }

    private void createNodePropertiesBatch(
            BatchInserter inserter,
            long[] nodeIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        switch ( propertyLocality )
        {
        case SCATTERED_BY_ELEMENT:
            createNodePropertiesScatteredBatch( inserter, nodeIds, propertyIndexReaders );
            break;
        case CO_LOCATED_BY_ELEMENT:
            createNodePropertiesSequentiallyBatch( inserter, nodeIds, propertyIndexReaders );
            break;
        default:
            throw new IllegalArgumentException( format( "Unexpected property locality: %s\nExpected one of: %s",
                                                        propertyLocality.name(),
                                                        Arrays.toString( PropertyLocality.values() ) ) );
        }
    }

    private void createNodePropertiesScatteredTx(
            GraphDatabaseService db,
            long[] nodeIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        int txStateCounter = 0;
        Transaction tx = db.beginTx();
        try
        {
            // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
            for ( int position = 0; position < nodePropertyValues.length; position++ )
            {
                IntFileReader propertyIndexReader = propertyIndexReaders[position];
                for ( int n = 0; n < nodes; n++ )
                {
                    long nodeId = nodeIds[n];
                    IntFileReader.assertAdvance( propertyIndexReader );
                    String key = nodePropertyKeys[propertyIndexReader.getInt()];
                    ValueGeneratorFun<?> value = nodePropertyValues[propertyIndexReader.getInt()];
                    Node node = tx.getNodeById( nodeId );
                    node.setProperty( key, value.next( rng ) );
                    if ( ++txStateCounter % TX_SIZE == 0 )
                    {
                        tx.commit();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
        }
        finally
        {
            tx.commit();
            tx.close();
        }
    }

    private void createNodePropertiesScatteredBatch(
            BatchInserter inserter,
            long[] nodeIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
        for ( int position = 0; position < nodePropertyValues.length; position++ )
        {
            IntFileReader propertyIndexReader = propertyIndexReaders[position];
            for ( int n = 0; n < nodes; n++ )
            {
                long nodeId = nodeIds[n];
                IntFileReader.assertAdvance( propertyIndexReader );
                String key = nodePropertyKeys[propertyIndexReader.getInt()];
                ValueGeneratorFun<?> value = nodePropertyValues[propertyIndexReader.getInt()];
                inserter.setNodeProperty( nodeId, key, value.next( rng ) );
            }
        }
    }

    private void createNodePropertiesSequentiallyTx(
            GraphDatabaseService db,
            long[] nodeIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        int txStateCounter = 0;
        Transaction tx = db.beginTx();
        try
        {
            // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
            for ( int n = 0; n < nodes; n++ )
            {
                long nodeId = nodeIds[n];
                for ( int position = 0; position < nodePropertyValues.length; position++ )
                {
                    IntFileReader propertyIndexReader = propertyIndexReaders[position];
                    IntFileReader.assertAdvance( propertyIndexReader );
                    String key = nodePropertyKeys[propertyIndexReader.getInt()];
                    ValueGeneratorFun<?> value = nodePropertyValues[propertyIndexReader.getInt()];
                    Node node = tx.getNodeById( nodeId );
                    node.setProperty( key, value.next( rng ) );
                    if ( ++txStateCounter % TX_SIZE == 0 )
                    {
                        tx.commit();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
        }
        finally
        {
            tx.commit();
            tx.close();
        }
    }

    private void createNodePropertiesSequentiallyBatch(
            BatchInserter inserter,
            long[] nodeIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
        for ( int n = 0; n < nodes; n++ )
        {
            long nodeId = nodeIds[n];
            for ( int position = 0; position < nodePropertyValues.length; position++ )
            {
                IntFileReader propertyIndexReader = propertyIndexReaders[position];
                IntFileReader.assertAdvance( propertyIndexReader );
                String key = nodePropertyKeys[propertyIndexReader.getInt()];
                ValueGeneratorFun<?> value = nodePropertyValues[propertyIndexReader.getInt()];
                inserter.setNodeProperty( nodeId, key, value.next( rng ) );
            }
        }
    }

    private void createRelationshipPropertiesTx(
            GraphDatabaseService db,
            IntFileReader relationshipIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        switch ( propertyLocality )
        {
        case SCATTERED_BY_ELEMENT:
            createRelationshipPropertiesScatteredTx( db, relationshipIds, propertyIndexReaders );
            break;
        case CO_LOCATED_BY_ELEMENT:
            createRelationshipPropertiesSequentiallyTx( db, relationshipIds, propertyIndexReaders );
            break;
        default:
            throw new IllegalArgumentException( format( "Unexpected property locality: %s\nExpected one of: %s",
                                                        propertyLocality.name(),
                                                        Arrays.toString( PropertyLocality.values() ) ) );
        }
    }

    private void createRelationshipPropertiesBatch(
            BatchInserter inserter,
            IntFileReader relationshipIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        switch ( propertyLocality )
        {
        case SCATTERED_BY_ELEMENT:
            createRelationshipPropertiesScatteredBatch( inserter, relationshipIds, propertyIndexReaders );
            break;
        case CO_LOCATED_BY_ELEMENT:
            createRelationshipPropertiesSequentiallyBatch( inserter, relationshipIds, propertyIndexReaders );
            break;
        default:
            throw new IllegalArgumentException( format( "Unexpected property locality: %s\nExpected one of: %s",
                                                        propertyLocality.name(),
                                                        Arrays.toString( PropertyLocality.values() ) ) );
        }
    }

    private void createRelationshipPropertiesScatteredTx(
            GraphDatabaseService db,
            IntFileReader relationshipIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        int txStateCounter = 0;
        Transaction tx = db.beginTx();
        try
        {
            for ( int position = 0; position < relationshipPropertyValues.length; position++ )
            {
                IntFileReader propertyIndexReader = propertyIndexReaders[position];
                relationshipIds.reset();
                while ( relationshipIds.advance() )
                {
                    long relationshipId = relationshipIds.getInt();
                    IntFileReader.assertAdvance( propertyIndexReader );
                    String key = relationshipPropertyKeys[propertyIndexReader.getInt()];
                    ValueGeneratorFun<?> value = relationshipPropertyValues[propertyIndexReader.getInt()];
                    Relationship relationship = tx.getRelationshipById( relationshipId );
                    relationship.setProperty( key, value.next( rng ) );
                    if ( ++txStateCounter % TX_SIZE == 0 )
                    {
                        tx.commit();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
        }
        finally
        {
            tx.commit();
            tx.close();
        }
    }

    private void createRelationshipPropertiesScatteredBatch(
            BatchInserter inserter,
            IntFileReader relationshipIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        for ( int position = 0; position < relationshipPropertyValues.length; position++ )
        {
            IntFileReader propertyIndexReader = propertyIndexReaders[position];
            relationshipIds.reset();
            while ( relationshipIds.advance() )
            {
                long relationshipId = relationshipIds.getInt();
                IntFileReader.assertAdvance( propertyIndexReader );
                String key = relationshipPropertyKeys[propertyIndexReader.getInt()];
                ValueGeneratorFun<?> value = relationshipPropertyValues[propertyIndexReader.getInt()];
                inserter.setRelationshipProperty( relationshipId, key, value.next( rng ) );
            }
        }
    }

    private void createRelationshipPropertiesSequentiallyTx(
            GraphDatabaseService db,
            IntFileReader relationshipIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        int txStateCounter = 0;
        Transaction tx = db.beginTx();
        try
        {
            relationshipIds.reset();
            while ( relationshipIds.advance() )
            {
                long relationshipId = relationshipIds.getInt();
                for ( int position = 0; position < relationshipPropertyValues.length; position++ )
                {
                    IntFileReader propertyIndexReader = propertyIndexReaders[position];
                    IntFileReader.assertAdvance( propertyIndexReader );
                    String key = relationshipPropertyKeys[propertyIndexReader.getInt()];
                    ValueGeneratorFun<?> value = relationshipPropertyValues[propertyIndexReader.getInt()];
                    Relationship relationship = tx.getRelationshipById( relationshipId );
                    relationship.setProperty( key, value.next( rng ) );
                    if ( ++txStateCounter % TX_SIZE == 0 )
                    {
                        tx.commit();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
        }
        finally
        {
            tx.commit();
            tx.close();
        }
    }

    private void createRelationshipPropertiesSequentiallyBatch(
            BatchInserter inserter,
            IntFileReader relationshipIds,
            IntFileReader[] propertyIndexReaders ) throws IOException
    {
        relationshipIds.reset();
        while ( relationshipIds.advance() )
        {
            long relationshipId = relationshipIds.getInt();
            for ( int position = 0; position < relationshipPropertyValues.length; position++ )
            {
                IntFileReader propertyIndexReader = propertyIndexReaders[position];
                IntFileReader.assertAdvance( propertyIndexReader );
                String key = relationshipPropertyKeys[propertyIndexReader.getInt()];
                ValueGeneratorFun<?> value = relationshipPropertyValues[propertyIndexReader.getInt()];
                inserter.setRelationshipProperty( relationshipId, key, value.next( rng ) );
            }
        }
    }

    private void createNodeLabels(
            GraphDatabaseService db,
            long[] nodeIds,
            IntFileReader[] labelIndexReaders ) throws IOException
    {
        switch ( labelLocality )
        {
        case SCATTERED_BY_NODE:
            createNodeLabelsScattered( db, nodeIds, labelIndexReaders );
            break;
        case CO_LOCATED_BY_NODE:
            createNodeLabelsSequentially( db, nodeIds, labelIndexReaders );
            break;
        default:
            throw new IllegalArgumentException( format( "Unexpected label locality: %s\nExpected one of: %s",
                                                        labelLocality.name(),
                                                        Arrays.toString( LabelLocality.values() ) ) );
        }
    }

    // use transactional API because batch inserter does not allow for labels to be added one at a time
    private void createNodeLabelsScattered(
            GraphDatabaseService db,
            long[] nodeIds,
            IntFileReader[] labelIndexReaders ) throws IOException
    {
        // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
        int txStateCounter = 0;
        Transaction tx = db.beginTx();
        try
        {
            for ( int position = 0; position < labels.length; position++ )
            {
                IntFileReader labelIndexReader = labelIndexReaders[position];
                for ( int n = 0; n < nodes; n++ )
                {
                    Node node = tx.getNodeById( nodeIds[n] );
                    IntFileReader.assertAdvance( labelIndexReader );
                    node.addLabel( labels[labelIndexReader.getInt()] );
                    if ( ++txStateCounter % TX_SIZE == 0 )
                    {
                        tx.commit();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
        }
        finally
        {
            tx.commit();
            tx.close();
        }
    }

    // use transactional API because batch inserter does not allow for labels to be added one at a time
    private void createNodeLabelsSequentially(
            GraphDatabaseService db,
            long[] nodeIds,
            IntFileReader[] labelIndexReaders ) throws IOException
    {
        // NOTE: do not assume ID space is continuous and monotonically increasing --> node ID array is required
        int txStateCounter = 0;
        Transaction tx = db.beginTx();
        try
        {
            for ( int n = 0; n < nodes; n++ )
            {
                Node node = tx.getNodeById( nodeIds[n] );
                for ( int position = 0; position < labels.length; position++ )
                {
                    IntFileReader labelIndexReader = labelIndexReaders[position];
                    IntFileReader.assertAdvance( labelIndexReader );
                    node.addLabel( labels[labelIndexReader.getInt()] );
                }
                if ( ++txStateCounter % TX_SIZE == 0 )
                {
                    tx.commit();
                    tx.close();
                    tx = db.beginTx();
                }
            }
        }
        finally
        {
            tx.commit();
            tx.close();
        }
    }

    private void createMandatoryNodeConstraints( GraphDatabaseService db )
    {
        Stream.of( mandatoryNodeConstraints )
              .forEach( def ->
                        {
                            assertIsNonComposite( def );
                            createMandatoryNodeConstraint( db, def.label(), def.keys()[0] );
                        } );
    }

    private void createMandatoryRelationshipConstraints( GraphDatabaseService db )
    {
        Stream.of( mandatoryRelationshipConstraints )
              .forEach( def -> createMandatoryRelationshipConstraint( db, def.type(), def.key() ) );
    }

    private void createUniquenessConstraints( GraphDatabaseService db )
    {
        Stream.of( uniqueConstraints )
              .forEach( def ->
                        {
                            assertIsNonComposite( def );
                            createUniquenessConstraint( db, def.label(), def.keys()[0] );
                        } );
    }

    private void createSchemaIndexes( GraphDatabaseService db )
    {
        Stream.of( schemaIndexes )
              .forEach( def -> createSchemaIndex( db, def.label(), def.keys() ) );
        Stream.of( fulltextNodeSchemaIndexes )
              .forEach( def -> createFulltextNodeIndex( db, def.label(), def.keys() ) );
        Stream.of( fulltextRelationshipSchemaIndexes )
              .forEach( def -> createFulltextRelationshipIndex( db, def.type(), def.key() ) );
    }

    public static void createMandatoryNodeConstraint( GraphDatabaseService db, Label label, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE CONSTRAINT ON (node:" + label + ") ASSERT exists(node.`" + key + "`)" );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Error creating mandatory node constraint (%s,%s)", label, key ), e );
        }
    }

    public static void createMandatoryRelationshipConstraint( GraphDatabaseService db, RelationshipType type,
                                                              String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE CONSTRAINT ON ()-[r:" + type + "]-() ASSERT exists(r.`" + key + "`)" );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException(
                    format( "Error creating mandatory relationship constraint (%s,%s)", type, key ), e );
        }
    }

    public static void createUniquenessConstraint( GraphDatabaseService db, Label label, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE CONSTRAINT ON (node:" + label + ") ASSERT node.`" + key + "` IS UNIQUE" );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Error creating uniqueness constraint on (%s,%s)", label, key ), e );
        }
    }

    public static void createSchemaIndex( GraphDatabaseService db, Label label, String... keys )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE " + compositeIndexPattern( label, keys ) );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Error creating composite schema index on (%s,%s)",
                                                label, Arrays.toString( keys ) ), e );
        }
    }

    public static void createFulltextNodeIndex( GraphDatabaseService db, Label label, String[] propertyKeys )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CALL db.index.fulltext.createNodeIndex('ftsNodes', ['" + label.name() + "'], ['" + join( "','", propertyKeys ) + "'] )" ).close();
            tx.commit();
        }
    }

    public static void createFulltextRelationshipIndex( GraphDatabaseService db, RelationshipType type, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CALL db.index.fulltext.createRelationshipIndex('ftsRels', ['" + type.name() + "'], ['" + propertyKey + "'])" ).close();
            tx.commit();
        }
    }

    public static void createNodeKey( GraphDatabaseService db, Label label, String... keys )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE " + compositeKeyPattern( label, keys ) );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Error creating composite key on (%s,%s)",
                                                label, Arrays.toString( keys ) ), e );
        }
    }

    public static void dropMandatoryNodeConstraint( GraphDatabaseService db, Label label, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "DROP CONSTRAINT ON (node:" + label + ") ASSERT exists(node.`" + key + "`)" );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException(
                    format( "Error creating mandatory node constraint (%s,%s)", label, key ), e );
        }
    }

    public static void dropMandatoryRelationshipConstraint( GraphDatabaseService db, Label label, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "DROP CONSTRAINT ON ()-[r:" + label + "]-() ASSERT exists(r.`" + key + "`)" );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException(
                    format( "Error creating mandatory relationship constraint (%s,%s)", label, key ), e );
        }
    }

    public static void dropUniquenessConstraint( GraphDatabaseService db, Label label, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "DROP CONSTRAINT ON (node:" + label + ") ASSERT node.`" + key + "` IS UNIQUE" );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Error creating uniqueness constraint on (%s,%s)", label, key ), e );
        }
    }

    public static void dropSchemaIndex( GraphDatabaseService db, Label label, String... keys )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "DROP " + compositeIndexPattern( label, keys ) );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Error creating composite schema index on (%s,%s)",
                                                label, Arrays.toString( keys ) ), e );
        }
    }

    public static void dropNodeKey( GraphDatabaseService db, Label label, String... keys )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "DROP " + compositeKeyPattern( label, keys ) );
            tx.commit();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( format( "Error creating composite key on (%s,%s)",
                                                label, Arrays.toString( keys ) ), e );
        }
    }

    public static void waitForSchemaIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            try
            {
                tx.schema().awaitIndexesOnline( 1, TimeUnit.DAYS );
            }
            catch ( Exception e )
            {
                throw indexWaitException( tx, e );
            }
        }
    }

    private static RuntimeException indexWaitException( Transaction tx, Exception e )
    {
        RuntimeException exception = new RuntimeException( "Error while waiting for indexes to come online", e );
        for ( IndexDefinition index : tx.schema().getIndexes() )
        {
            Schema.IndexState indexState = tx.schema().getIndexState( index );
            if ( indexState == Schema.IndexState.FAILED )
            {
                exception.addSuppressed( new RuntimeException( "Index " + index + " failed: " + tx.schema().getIndexFailure( index ) ) );
            }
            else if ( indexState == Schema.IndexState.POPULATING )
            {
                exception.addSuppressed( new RuntimeException( "Index is still building: " + index ) );
            }
        }
        return exception;
    }

    public static void waitForSchemaIndexes( GraphDatabaseService db, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            try
            {
                var schema = tx.schema();
                for ( IndexDefinition index : schema.getIndexes( label ) )
                {
                    schema.awaitIndexOnline( index, 1, TimeUnit.DAYS );
                }
            }
            catch ( Exception e )
            {
                throw indexWaitException( tx, e );
            }
        }
    }

    private static String compositeIndexPattern( Label label, String... keys )
    {
        String keysString = Arrays.stream( keys ).map( key -> "`" + key + "`" ).collect( joining( "," ) );
        return "INDEX ON :" + label + "(" + keysString + ")";
    }

    private static String compositeKeyPattern( Label label, String... keys )
    {
        String keysString = Arrays.stream( keys ).map( key -> "n.`" + key + "`" ).collect( joining( "," ) );
        return "CONSTRAINT ON (n:" + label + ") ASSERT (" + keysString + ") IS NODE KEY";
    }

    private void assertIsNonComposite( LabelKeyDefinition labelKeyDef )
    {
        if ( labelKeyDef.keys().length != 1 )
        {
            throw new RuntimeException( format( "Expected non-composite index pattern, but found: %s", labelKeyDef ) );
        }
    }
}
