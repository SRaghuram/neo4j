/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j;

import com.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;

import java.io.File;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.values.storable.DateValue;

public class Tester
{
    //22343
    //-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=delay=10s,duration=60s,filename=myrecording.jfr

    public static void main( String[] args )
    {
        GraphDatabaseService db = new EnterpriseGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( new File( "/Users/pontusmelke/tmp/db" ) )
                //  .setConfig( GraphDatabaseSettings.allow_upgrade, "true" )
                .newGraphDatabase();

        String query1 =
                "CYPHER runtime=slotted expressionEngine=interpreted UNWIND range(1, 10000) AS r RETURN sin(1.0) ";
                //"CYPHER runtime=slotted MATCH (n: Test), (m: Test) WHERE n.dateString < m.dateString RETURN n.date";
        String query2 =
                "CYPHER runtime=slotted expressionEngine=interpreted UNWIND range(1, 10000) AS r RETURN sin2( 1.0 ) ";
                //"CYPHER runtime=slotted MATCH (n: Test), (m: Test) WHERE n.date < m.date RETURN n.date";
        String query3 =
                "CYPHER runtime=slotted MATCH (n: Test), (m: Test) WHERE date(n.dateString) < date(m.dateString) " +
                "RETURN n.date";

        try
        {
            // setup( db );
//        Result execute1 = db.execute( "PROFILE " + query1 );
//        execute1.resultAsString();
//        System.out.println( execute1.getExecutionPlanDescription());
//
//        Result execute2 = db.execute( "PROFILE " + query2 );
//        execute2.resultAsString();
//        System.out.println( execute2.getExecutionPlanDescription());
//            Result execute3 = db.execute( "PROFILE " + query3 );
//            execute3.resultAsString();
//            System.out.println( execute3.getExecutionPlanDescription() );

            //warmup
            runNTimes( db, query1, 100 );
            runNTimes( db, query2, 100 );
//            runNTimes( db, query3, 100 );

            long tic = System.currentTimeMillis();
            runNTimes( db, query1, 1000 );
            System.out.println( System.currentTimeMillis() - tic );

            tic = System.currentTimeMillis();
            runNTimes( db, query2, 1000 );
            System.out.println( System.currentTimeMillis() - tic );
//
//            tic = System.currentTimeMillis();
//            //runNTimes( db, query1, 5000 );
//            System.out.println( System.currentTimeMillis() - tic );

        }
        finally
        {
            db.shutdown();

        }
    }

    private static boolean runNTimes( GraphDatabaseService db, String query, int n )
    {
        boolean empty = false;
        for ( int i = 0; i < n; i++ )
        {
            int count = 0;
            Result execute = db.execute( query );
            while ( execute.hasNext() )
            {
                execute.next();
                count++;
            }
            empty = count == 0;
        }
        return empty;
    }

    private static void setup( GraphDatabaseService db )
    {

        try ( Transaction tx = db.beginTx() )
        {
            ThreadLocalRandom current = ThreadLocalRandom.current();
            Map<String,Object> params = new HashMap<>();
            for ( int i = 0; i < 100; i++ )
            {
                String dateString = String.format( "20%02d-01-01", current.nextInt( 1, 32 ) );
                //String dateString = String.format( "2019-01-%02d", current.nextInt( 1, 32 ) );
                System.out.println( dateString );
                params.put( "date", dateString );
                db.execute( "CREATE (:Test {dateString: $date, date: date($date)})", params );
            }
            tx.success();
        }
    }
}
