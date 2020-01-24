/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.util;

import org.apache.commons.lang3.mutable.MutableInt;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.time.Stopwatch;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;

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
            for ( int i = 0; i < 5; i++ )
            {
                runBenchmark( db, 0xDEAD + i );
            }
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



    private static final int NUM_CITIES = 100_000;
    private static final int NUM_CONNECTIONS_PER_CIRY = 6;

    private static final Label CITY = Label.label( "City" );
    private static final RelationshipType ROAD = RelationshipType.withName( "Road" );
    private static final String DISTANCE = "distance";
    private static final String SORTVALUE = "sort";
    private static final int DISTANCE_VARIATION = 10;


    private static final int TRAVEL_HOPS = 10_000_000;
    private static long startCityID;


    private static void createData( GraphDatabaseService db, int seed )
    {

        System.out.println( "Data creation started using seed " + seed);
        Random random = new Random( seed );
        try ( Transaction tx = db.beginTx() )
        {
            List<Node> cities = new ArrayList<>( NUM_CITIES );
            HashMap<Node,MutableInt> roadPerCity = new HashMap<>();
            for ( int i = 0; i < NUM_CITIES; i++ )
            {
                Node city = tx.createNode( CITY );
                cities.add( city );
                roadPerCity.put( city, new MutableInt( 0 ) );
            }
            MutableInt count = new MutableInt( 0 );
            MutableInt lastPercent = new MutableInt( -1 );
            MutableInt relationshipCounter = new MutableInt( 0 );
            roadPerCity.forEach( ( city, numRoads ) ->
            {
                while ( numRoads.getValue() < NUM_CONNECTIONS_PER_CIRY / 2 )
                {
                    Node otherCity = cities.get( random.nextInt( cities.size() ) );
                    MutableInt otherCityNumRoads = roadPerCity.get( otherCity );
                    if ( otherCityNumRoads.getValue() < NUM_CONNECTIONS_PER_CIRY - 1)
                    {
                        otherCityNumRoads.increment();
                        numRoads.increment();
                        Relationship road = city.createRelationshipTo( otherCity, ROAD );
                        road.setProperty( DISTANCE, random.nextInt( DISTANCE_VARIATION ) + 1 );
                        road.setProperty( SORTVALUE, relationshipCounter.incrementAndGet() );
                    }
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

    private static void runBenchmark( GraphDatabaseService db, int seed )
    {
        System.out.println( "Benchmark started using seed: " + seed);
        Random random = new Random( seed );
        Stopwatch time = Stopwatch.start();
        long distance = 0;
        try ( Transaction tx = db.beginTx() )
        {
            Node city = tx.getNodeById( startCityID );
            int hops = 0;

            while ( hops++ < TRAVEL_HOPS )
            {
                Map<Integer,Relationship> sortedRoads = new TreeMap<>();
                city.getRelationships().forEach( r -> sortedRoads.put( (int) r.getProperty( SORTVALUE ), r ));
                Relationship[] relationships = sortedRoads.values().toArray( new Relationship[0] );
                Relationship road = relationships[random.nextInt( sortedRoads.size() )];
                city = road.getOtherNode( city );
                distance += (int) road.getProperty( DISTANCE );
            }
            tx.commit();
        }
        System.out.println( "Benchmark ended. Distance traveled " + distance + " Time: " + time.elapsed() + " Hops: " + TRAVEL_HOPS );
    }
}
