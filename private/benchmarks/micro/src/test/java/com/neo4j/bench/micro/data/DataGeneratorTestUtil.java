/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.util.BenchmarkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;

import static com.neo4j.bench.micro.data.DataGenerator.GraphWriter.BATCH;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.number.IsCloseTo.closeTo;

public class DataGeneratorTestUtil
{
    private static final Logger LOG = LoggerFactory.getLogger( DataGeneratorTestUtil.class );
    private static final Path NEO4J_CONFIG = Paths.get( "neo4j.config" );

    static void assertGraphStatsAreConsistentWithBuilderConfiguration(
            Store store,
            DataGeneratorConfigBuilder builder,
            double percentageTolerance ) throws IOException
    {
        DataGeneratorConfig config = builder.build();
        DataGenerator generator = new DataGenerator( config );
        LOG.debug( config.toString() );

        BenchmarkUtil.forceRecreateFile( NEO4J_CONFIG );
        Neo4jConfigBuilder.writeToFile( config.neo4jConfig(), NEO4J_CONFIG );

        generator.generate( store, NEO4J_CONFIG );

        LOG.debug( "Tolerance: " + percentageTolerance );

        GraphDatabaseService db = null;
        try
        {
            db = ManagedStore.newDb( store, NEO4J_CONFIG );

            // check global counts
            assertThat( nodeCount( db ), equalTo( config.nodeCount() ) );
            assertThat( relationshipCount( db ), equalTo( config.relationshipCount() ) );
            assertThat( nodePropertyCount( db ), equalTo( config.nodePropertyCount() ) );
            assertThat( relationshipPropertyCount( db ), equalTo( config.relationshipPropertyCount() ) );

            // check degree distribution
            assertDegreesAreWithinTolerance( db, config.outDegree(), percentageTolerance );

            // check property chain ordering
            List<ChainPosition> nodePropertyChainPositions = computeNodePropertyChainsStats( db );
            List<ChainPosition> relationshipPropertyChainPositions = computeRelationshipPropertyChainsStats( db );
            // print property chain ordering stats
            printChainStats( nodePropertyChainPositions, true );
            printChainStats( relationshipPropertyChainPositions, true );
            switch ( config.propertyOrder() )
            {
            case ORDERED:
                assertChainsAreOrdered( nodePropertyChainPositions, percentageTolerance );
                assertChainsAreOrdered( relationshipPropertyChainPositions, percentageTolerance );
                break;
            case SHUFFLED:
                if ( config.graphWriter().equals( BATCH ) )
                {
                    assertChainsAreShuffled( nodePropertyChainPositions, percentageTolerance );
                    assertChainsAreShuffled( relationshipPropertyChainPositions, percentageTolerance );
                }
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized property order: " +
                                                    config.propertyOrder() );
            }

            // check relationship chain ordering
            List<ChainPosition> relationshipTypeChainPositions = computeRelationshipTypeChainsStats( db );
            // print relationship chain ordering stats
            printChainStats( relationshipTypeChainPositions, true );
            switch ( config.relationshipOrder() )
            {
            case ORDERED:
                assertChainsAreOrdered( relationshipTypeChainPositions, percentageTolerance );
                break;
            case SHUFFLED:
                assertChainsAreShuffled( relationshipTypeChainPositions, percentageTolerance );
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized relationship order: " +
                                                    config.relationshipOrder() );
            }

            // TODO NOTE: does not work. labels always returned in same order. product performs a sort
//            // check label "chain" ordering
//            List<ChainPosition> nodeLabelChainPositions = computeNodeLabelChainsStats( db );
//            // print label ordering stats
//            printChainStats( nodeLabelChainPositions, true );
//            switch ( config.labelOrder() )
//            {
//            case ORDERED:
//                assertChainsAreOrdered( nodeLabelChainPositions, percentageTolerance );
//                break;
//            case SHUFFLED:
//                assertChainsAreShuffled( nodeLabelChainPositions, percentageTolerance );
//                break;
//            default:
//                throw new IllegalArgumentException( "Unrecognized label order: " +
//                                                    config.labelOrder() );
//            }

            // check relationship locality
            switch ( config.relationshipLocality() )
            {
            case CO_LOCATED_BY_START_NODE:
                assertRelationshipsAreCollocatedByStartNode( db );
                break;
            case SCATTERED_BY_START_NODE:
                assertRelationshipsAreScatteredByStartNode( db );
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized relationship locality: " +
                                                    config.relationshipLocality() );
            }

            // TODO NOTE: do not know how to test label locality, as labels do not have identifiers

            // TODO NOTE: do not know how to test property locality, as properties do not have identifiers
        }
        finally
        {
            FileUtils.deleteFile( NEO4J_CONFIG );
            if ( null != db )
            {
                ManagedStore.getManagementService().shutdown();
            }
        }
    }

    private static int nodeCount( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            return Iterables.count( transaction.getAllNodes() );
        }
    }

    private static int relationshipCount( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            return Iterables.count( transaction.getAllRelationships() );
        }
    }

    private static void assertDegreesAreWithinTolerance(
            GraphDatabaseService db,
            int outRelationshipsPerNode,
            double percentageTolerance )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // only run tests if graph actually has relationships
            if ( Iterables.count( transaction.getAllRelationships() ) == 0 )
            {
                return;
            }
            Histogram inDegreeHistogram = new Histogram( new UniformReservoir() );
            Histogram outDegreeHistogram = new Histogram( new UniformReservoir() );
            Histogram degreeHistogram = new Histogram( new UniformReservoir() );

            for ( Node node : transaction.getAllNodes() )
            {
                inDegreeHistogram.update( node.getDegree( Direction.INCOMING ) );
                outDegreeHistogram.update( node.getDegree( Direction.OUTGOING ) );
                degreeHistogram.update( node.getDegree() );
            }

            double degreeError = outRelationshipsPerNode * percentageTolerance;

            assertThat( format( "Expect mean in-degree in range [%s, %s]",
                                inDegreeHistogram.getSnapshot().getMean() - degreeError,
                                inDegreeHistogram.getSnapshot().getMean() + degreeError ),
                        inDegreeHistogram.getSnapshot().getMean(),
                        closeTo( outRelationshipsPerNode, degreeError ) );
            assertThat( format( "Expect mean out-degree in range [%s, %s]",
                                outDegreeHistogram.getSnapshot().getMean() - degreeError,
                                outDegreeHistogram.getSnapshot().getMean() + degreeError ),
                        outDegreeHistogram.getSnapshot().getMean(),
                        closeTo( outRelationshipsPerNode, degreeError ) );
            assertThat( format( "Expect mean degree in range [%s, %s]",
                                degreeHistogram.getSnapshot().getMean() - degreeError,
                                degreeHistogram.getSnapshot().getMean() + degreeError ),
                        degreeHistogram.getSnapshot().getMean(),
                        closeTo( (double) outRelationshipsPerNode * 2, degreeError ) );

            assertThat( format( "Expect 95th percentile < 2 x mean: in-degree < %s",
                                inDegreeHistogram.getSnapshot().getMean() * 2 ),
                        inDegreeHistogram.getSnapshot().getValue( 0.95 ),
                        lessThan( inDegreeHistogram.getSnapshot().getMean() * 2 ) );
            assertThat( format( "Expect 95th percentile < 2 x mean: out-degree < %s",
                                outDegreeHistogram.getSnapshot().getMean() * 2 ),
                        outDegreeHistogram.getSnapshot().getValue( 0.95 ),
                        lessThan( outDegreeHistogram.getSnapshot().getMean() * 2 ) );
            assertThat( format( "Expect 95th percentile < 2 mean: degree < %s",
                                degreeHistogram.getSnapshot().getMean() * 2 ),
                        degreeHistogram.getSnapshot().getValue( 0.95 ),
                        lessThan( degreeHistogram.getSnapshot().getMean() * 2 ) );
        }
    }

    private static int nodePropertyCount( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            return propertyCount( transaction.getAllNodes() );
        }
    }

    private static int relationshipPropertyCount( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            return propertyCount( transaction.getAllRelationships() );
        }
    }

    private static int propertyCount( Iterable<? extends Entity> entities )
    {
        return StreamSupport.stream( entities.spliterator(), false )
                            .map( n -> Iterables.count( n.getPropertyKeys() ) )
                            .reduce( 0, Integer::sum );
    }

    private static void assertRelationshipsAreCollocatedByStartNode( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            long previousStartNodeId = -1;
            for ( Relationship relationship : transaction.getAllRelationships() )
            {
                long currentStartNodeId = relationship.getStartNode().getId();
                assertThat( currentStartNodeId,
                            anyOf( equalTo( previousStartNodeId ), equalTo( previousStartNodeId + 1 ) ) );
                previousStartNodeId = currentStartNodeId;
            }
        }
    }

    private static void assertRelationshipsAreScatteredByStartNode( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            long previousStartNodeId = -1;
            for ( Relationship relationship : transaction.getAllRelationships() )
            {
                long currentStartNodeId = relationship.getStartNode().getId();
                assertThat( currentStartNodeId,
                            anyOf( equalTo( previousStartNodeId + 1 ), equalTo( 0L ) ) );
                previousStartNodeId = currentStartNodeId;
            }
        }
    }

    private static List<ChainPosition> computeNodePropertyChainsStats( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            return computeChainsStats( transaction.getAllNodes(), Entity::getPropertyKeys );
        }
    }

    private static List<ChainPosition> computeRelationshipPropertyChainsStats( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            return computeChainsStats( transaction.getAllRelationships(), Entity::getPropertyKeys );
        }
    }

    private static List<ChainPosition> computeRelationshipTypeChainsStats( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            return computeChainsStats(
                    tx.getAllNodes(),
                    n -> StreamSupport
                            .stream( n.getRelationships( Direction.OUTGOING ).spliterator(), false )
                            .map( Relationship::getType )
                            .map( RelationshipType::name )
                            .collect( Collectors.toList() ) );
        }
    }

    public static List<ChainPosition> computeNodeLabelChainsStats( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            return computeChainsStats(
                    tx.getAllNodes(),
                    // return list of label names
                    n -> StreamSupport
                            .stream( n.getLabels().spliterator(), false )
                            .map( Label::name )
                            .collect( Collectors.toList() )
            );
        }
    }

    private static <T> List<ChainPosition> computeChainsStats(
            Iterable<T> graphElements,
            Function<T,Iterable<String>> elementToKeysFun )
    {
        List<Map<String,Integer>> keyCountsAtChainPosition = new ArrayList<>();
        for ( T graphElement : graphElements )
        {
            int chainPosition = 0;
            for ( String key : elementToKeysFun.apply( graphElement ) )
            {
                if ( keyCountsAtChainPosition.size() <= chainPosition )
                {
                    keyCountsAtChainPosition.add( new HashMap<>() );
                }
                Map<String,Integer> keyCounts = keyCountsAtChainPosition.get( chainPosition );
                if ( !keyCounts.containsKey( key ) )
                {
                    keyCounts.put( key, 0 );
                }
                keyCounts.put( key, keyCounts.get( key ) + 1 );
                chainPosition++;
            }
        }

        List<ChainPosition> chainPositionList = new ArrayList<>();
        Set<String> keys = keyCountsAtChainPosition.stream().map( Map::keySet ).reduce( new HashSet<>(),
                                                                                        ( acc, s ) ->
                                                                                        {
                                                                                            acc.addAll( s );
                                                                                            return acc;
                                                                                        } );
        int position = 0;
        for ( Map<String,Integer> keyCountMap : keyCountsAtChainPosition )
        {
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            int total = 0;
            for ( String key : keys )
            {
                int value = keyCountMap.getOrDefault( key, 0 );
                min = Math.min( min, value );
                max = Math.max( max, value );
                total += value;
            }
            double mean = (double) total / keyCountMap.size();
            double error = (double) (max - min) / 2;
            List<KeyCount> keyCountsList = keyCountMap.entrySet().stream()
                                                      .map( e -> new KeyCount( e.getKey(), e.getValue() ) )
                                                      .collect( Collectors.toList() );
            chainPositionList.add( new ChainPosition( position, min, max, mean, error, keyCountsList ) );
            position++;
        }
        return chainPositionList;
    }

    public static void assertChainsAreShuffled( List<ChainPosition> chainPositionList, double tolerancePercentage )
    {
        assertThat(
                "Tolerance percentage must be in range [0.0, 1.0]",
                tolerancePercentage,
                allOf( greaterThanOrEqualTo( 0.0 ), lessThanOrEqualTo( 1.0 ) ) );

        for ( ChainPosition stats : chainPositionList )
        {
            double error = tolerancePercentage * stats.mean();
            assertThat( stats.toString(), stats.error(), lessThanOrEqualTo( error ) );
            assertThat( format( "Expected MIN to be in [%s, %s], but was: %s\n%s\n%s",
                                stats.mean() - error, stats.mean() + error, stats.min(), stats.toString(),
                                stats.keyCountsString() ),
                        (double) stats.min(), closeTo( stats.mean(), error ) );
            assertThat( format( "Expected MAX to be in [%s, %s], but was: %s\n%s\n%s",
                                stats.mean() - error, stats.mean() + error, stats.max(), stats.toString(),
                                stats.keyCountsString() ),
                        (double) stats.max(), closeTo( stats.mean(), error ) );
        }
    }

    private static void assertChainsAreOrdered( List<ChainPosition> chainPositionList, double tolerancePercentage )
    {
        assertThat(
                "Tolerance percentage must be in range [0.0, 1.0]",
                tolerancePercentage,
                allOf( greaterThanOrEqualTo( 0.0 ), lessThanOrEqualTo( 1.0 ) ) );

        List<String> dominantKeysPerPosition = new ArrayList<>();

        for ( ChainPosition stats : chainPositionList )
        {
            int keysNearMax = 0;
            int keysNearMin = 0;
            int keysInMiddle = 0;
            double error = tolerancePercentage * stats.mean();
            KeyCount maxKeyCount = null;
            for ( KeyCount keyCount : stats.keyCounts() )
            {
                if ( null == maxKeyCount || keyCount.count() > maxKeyCount.count() )
                {
                    maxKeyCount = keyCount;
                }
                boolean keyIsNearMinOrMax = false;
                if ( Math.abs( keyCount.count() - stats.max() ) < error )
                {
                    keysNearMax++;
                    keyIsNearMinOrMax = true;
                }
                if ( Math.abs( keyCount.count() - stats.min() ) < error )
                {
                    keysNearMin++;
                    keyIsNearMinOrMax = true;
                }
                if ( !keyIsNearMinOrMax )
                {
                    keysInMiddle++;
                }
            }
            assertThat( keysNearMin, equalTo( stats.keyCounts().size() - 1 ) );
            assertThat( keysNearMax, equalTo( 1 ) );
            assertThat( keysInMiddle, equalTo( 0 ) );

            if ( null != maxKeyCount )
            {
                dominantKeysPerPosition.add( maxKeyCount.key() );
            }
        }

        assertThat( "At each position a different key should be dominant: " + dominantKeysPerPosition,
                    dominantKeysPerPosition.size(),
                    equalTo( Sets.newHashSet( dominantKeysPerPosition ).size() ) );
    }

    private static void printChainStats( List<ChainPosition> chainPositionList, boolean detailed )
    {
        if ( chainPositionList.isEmpty() )
        {
            return;
        }
        StringBuilder sb = new StringBuilder( "Chain Stats:\n" );
        for ( ChainPosition stats : chainPositionList )
        {
            sb.append( "\t" ).append( stats.toString() ).append( "\n" );
            if ( detailed )
            {
                sb.append( "\t" ).append( stats.keyCountsString() ).append( "\n" );
            }
            sb.append( "\n" );
        }
        LOG.debug( sb.toString() );
    }

    public static class KeyCount
    {
        private final String key;
        private final Integer count;

        KeyCount( String key, Integer count )
        {
            this.key = key;
            this.count = count;
        }

        String key()
        {
            return key;
        }

        Integer count()
        {
            return count;
        }

        @Override
        public String toString()
        {
            return "('" + key + "'," + count + ')';
        }
    }

    public static class ChainPosition
    {
        private final int position;
        private final long min;
        private final long max;
        private final double mean;
        private final double error;
        private final List<KeyCount> keyCounts;

        ChainPosition( int position, long min, long max, double mean, double error, List<KeyCount> keyCounts )
        {
            this.position = position;
            this.min = min;
            this.max = max;
            this.mean = mean;
            this.error = error;
            this.keyCounts = keyCounts;
        }

        int position()
        {
            return position;
        }

        long min()
        {
            return min;
        }

        long max()
        {
            return max;
        }

        double mean()
        {
            return mean;
        }

        double error()
        {
            return error;
        }

        List<KeyCount> keyCounts()
        {
            return keyCounts;
        }

        String keyCountsString()
        {
            StringBuilder sb = new StringBuilder( "[ " );
            for ( KeyCount keyCount : keyCounts() )
            {
                sb.append( "(" ).append( keyCount.key() ).append( "," ).append( keyCount.count() ).append( ") " );
            }
            return sb.append( "]" ).toString();
        }

        @Override
        public String toString()
        {
            return format( "pos[%s] --> [%s, %s], mean: %s, error: %s, %s",
                           position(), min(), max(), mean(), error(),
                           keyCounts().stream().map( KeyCount::toString ).collect( Collectors.toList() ).toString() );
        }
    }
}
