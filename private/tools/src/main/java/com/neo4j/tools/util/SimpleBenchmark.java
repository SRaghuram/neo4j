/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.util;

import org.apache.commons.lang3.mutable.MutableInt;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.BiFunction;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.time.Stopwatch;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.IntValue;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

public class SimpleBenchmark
{
    public static void main( String[] args ) throws Exception
    {
        File homeDirectory = new File( "/home/roxling/Desktop/test" );
        FileUtils.deleteRecursively( homeDirectory );
        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder( homeDirectory )
                .setConfig( tx_state_max_off_heap_memory, ByteUnit.gibiBytes( 10 ) )
                .setConfig( pagecache_memory, "10G" )
                .build();
        try
        {
            GraphDatabaseService db = dbms.database( DEFAULT_DATABASE_NAME );
            createData( db, 0xBEEF );
            long baseBenchmarkSeed = 0xDEAD;
            runBenchmarkACoupleOfTimes( "Core API", db, baseBenchmarkSeed, SimpleBenchmark::runBenchmark );
            runBenchmarkACoupleOfTimes( "Kernel Cursors", db, baseBenchmarkSeed, SimpleBenchmark::runBenchmarkWithKernelCursors );
            runBenchmarkACoupleOfTimes( "Storage Cursors", db, baseBenchmarkSeed, SimpleBenchmark::runBenchmarkWithStorageCursors );
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
            throw t;
        }
        finally
        {
            dbms.shutdown();
        }
    }

    private static void runBenchmarkACoupleOfTimes( String name, GraphDatabaseService db, long baseBenchmarkSeed,
            BiFunction<GraphDatabaseService,Random,Long> benchmark )
    {
        for ( int i = 0; i < 5; i++ )
        {
            long seed = baseBenchmarkSeed + i;
            System.out.println( "Benchmark '" + name + "' started using seed: " + seed );
            Stopwatch time = Stopwatch.start();
            long distance = benchmark.apply( db, new Random( seed ) );
            System.out.println( "Benchmark '" + name + "' ended. Distance traveled " + distance + " Time: " + time.elapsed() + " Hops: " + TRAVEL_HOPS );
        }
    }

    private static final int NUM_CITIES = 100_000;

    private static final Label CITY = Label.label( "City" );
    private static final RelationshipType ROAD = RelationshipType.withName( "Road" );
    private static final String DISTANCE = "distance";
    private static final String SORTVALUE = "sort";
    private static final int DISTANCE_VARIATION = 10;

    private static final int TRAVEL_HOPS = 1_000_000;
    private static long startCityID;

    private static void createData( GraphDatabaseService db, int seed )
    {
        System.out.println( "Data creation started using seed " + seed );
        Random random = new Random( seed );
        try ( Transaction tx = db.beginTx() )
        {
            List<Node> cities = new ArrayList<>( NUM_CITIES );
            for ( int i = 0; i < NUM_CITIES; i++ )
            {
                Node city = tx.createNode( CITY );
                cities.add( city );
            }
            MutableInt count = new MutableInt( 0 );
            MutableInt lastPercent = new MutableInt( -1 );
            MutableInt relationshipCounter = new MutableInt( 0 );
            cities.forEach( city ->
            {
                // Create roads for this node
                int numberOfRoadsToCreate = random.nextInt( 10 ) + 1;
                for ( int i = 0; i < numberOfRoadsToCreate; i++ )
                {
                    Node otherCity = cities.get( random.nextInt( cities.size() ) );
                    Relationship road = city.createRelationshipTo( otherCity, ROAD );
                    road.setProperty( DISTANCE, random.nextInt( DISTANCE_VARIATION ) + 1 );
                    road.setProperty( SORTVALUE, relationshipCounter.incrementAndGet() );
                }

                int currentPercent = (int) (count.getAndIncrement() * 100.0 / NUM_CITIES);
                if ( lastPercent.getValue() < currentPercent )
                {
                    lastPercent.setValue( currentPercent );
                    System.out.println( currentPercent + "%" );
                }
            } );

            startCityID = cities.get( random.nextInt( cities.size() ) ).getId();
            tx.commit();
        }
        System.out.println( "Data creation finished" );
    }

    private static long runBenchmark( GraphDatabaseService db, Random random )
    {
        long distance = 0;
        try ( Transaction tx = db.beginTx() )
        {
            Node city = tx.getNodeById( startCityID );
            int hops = 0;

            while ( hops++ < TRAVEL_HOPS )
            {
                Map<Integer,Relationship> sortedRoads = new TreeMap<>();
                city.getRelationships().forEach( r -> sortedRoads.put( (int) r.getProperty( SORTVALUE ), r ) );
                Relationship[] relationships = sortedRoads.values().toArray( new Relationship[0] );
                Relationship road = relationships[random.nextInt( sortedRoads.size() )];
                city = road.getOtherNode( city );
                distance += (int) road.getProperty( DISTANCE );
            }
            tx.commit();
        }
        return distance;
    }

    private static long runBenchmarkWithKernelCursors( GraphDatabaseService db, Random random )
    {
        long distance = 0;
        DependencyResolver deps = ((GraphDatabaseAPI) db).getDependencyResolver();
        TokenHolders tokenHolders = deps.resolveDependency( TokenHolders.class );
        int sortKey = tokenHolders.propertyKeyTokens().getIdByName( SORTVALUE );
        int distanceKey = tokenHolders.propertyKeyTokens().getIdByName( DISTANCE );
        Kernel kernel = deps.resolveDependency( Kernel.class );
        try ( KernelTransaction tx = kernel.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED );
                NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( NULL );
                RelationshipTraversalCursor relationshipCursor = tx.cursors().allocateRelationshipTraversalCursor( NULL );
                PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor( NULL ) )
        {
            tx.dataRead().singleNode( startCityID, nodeCursor );
            nodeCursor.next();
            int hops = 0;
            while ( hops++ < TRAVEL_HOPS )
            {
                Map<Integer,long[]> sortedRoads = new TreeMap<>();
                nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
                while ( relationshipCursor.next() )
                {
                    relationshipCursor.properties( propertyCursor );
                    int roadSortValue = 0;
                    int roadDistance = 0;
                    while ( propertyCursor.next() )
                    {
                        if ( propertyCursor.propertyKey() == sortKey )
                        {
                            roadSortValue = ((IntValue) propertyCursor.propertyValue()).value();
                        }
                        else if ( propertyCursor.propertyKey() == distanceKey )
                        {
                            roadDistance = ((IntValue) propertyCursor.propertyValue()).value();
                        }
                    }
                    sortedRoads.put( roadSortValue, new long[]{roadDistance, relationshipCursor.otherNodeReference()} );
                }
                long[][] roads = sortedRoads.values().toArray( new long[sortedRoads.size()][] );
                long[] road = roads[random.nextInt( sortedRoads.size() )];
                tx.dataRead().singleNode( road[1], nodeCursor );
                nodeCursor.next();
                distance += road[0];
            }
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
        return distance;
    }

    private static long runBenchmarkWithStorageCursors( GraphDatabaseService db, Random random )
    {
        long distance = 0;
        DependencyResolver deps = ((GraphDatabaseAPI) db).getDependencyResolver();
        TokenHolders tokenHolders = deps.resolveDependency( TokenHolders.class );
        int sortKey = tokenHolders.propertyKeyTokens().getIdByName( SORTVALUE );
        int distanceKey = tokenHolders.propertyKeyTokens().getIdByName( DISTANCE );
        try ( StorageReader reader = deps.resolveDependency( StorageEngine.class ).newReader();
                StorageNodeCursor nodeCursor = reader.allocateNodeCursor( NULL );
                StorageRelationshipTraversalCursor relationshipCursor = reader.allocateRelationshipTraversalCursor( NULL );
                StoragePropertyCursor propertyCursor = reader.allocatePropertyCursor( NULL ) )
        {
            nodeCursor.single( startCityID );
            nodeCursor.next();
            int hops = 0;
            while ( hops++ < TRAVEL_HOPS )
            {
                Map<Integer,long[]> sortedRoads = new TreeMap<>();
                nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
                while ( relationshipCursor.next() )
                {
                    relationshipCursor.properties( propertyCursor );
                    int roadSortValue = 0;
                    int roadDistance = 0;
                    while ( propertyCursor.next() )
                    {
                        if ( propertyCursor.propertyKey() == sortKey )
                        {
                            roadSortValue = ((IntValue) propertyCursor.propertyValue()).value();
                        }
                        else if ( propertyCursor.propertyKey() == distanceKey )
                        {
                            roadDistance = ((IntValue) propertyCursor.propertyValue()).value();
                        }
                    }
                    sortedRoads.put( roadSortValue, new long[]{roadDistance, relationshipCursor.neighbourNodeReference()} );
                }
                long[][] roads = sortedRoads.values().toArray( new long[sortedRoads.size()][] );
                long[] road = roads[random.nextInt( sortedRoads.size() )];
                nodeCursor.single( road[1] );
                nodeCursor.next();
                distance += road[0];
            }
        }
        return distance;
    }
}
