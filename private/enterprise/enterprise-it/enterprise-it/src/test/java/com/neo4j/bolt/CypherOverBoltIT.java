/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.rule.SuppressOutput;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CypherOverBoltIT
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Rule
    public Neo4jRule graphDb = new Neo4jRule();

    private URL url;
    private final int lineCountInCSV = 3; // needs to be >= 2

    @Before
    public void setUp() throws Exception
    {
        url = prepareTestImportFile( lineCountInCSV );
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWork()
    {

        for ( int i = lineCountInCSV - 1; i < lineCountInCSV + 1; i++ ) // test with different periodic commit sizes
        {
            try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
                    Session session = driver.session() )
            {
                StatementResult result = session.run( "USING PERIODIC COMMIT " + i + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                        "MERGE (currentnode:Label1 {uuid:row[0]})\n" + "RETURN currentnode;" );
                int countOfNodes = 0;
                while ( result.hasNext() )
                {
                    Node node = result.next().get( 0 ).asNode();
                    assertTrue( node.hasLabel( "Label1" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
                assertEquals( lineCountInCSV, countOfNodes );
                session.reset();
            }
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWork2()
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
                Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV + 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label1 {uuid:row[0]})\n" + "RETURN currentnode;" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Node node = result.next().get( 0 ).asNode();
                assertTrue( node.hasLabel( "Label1" ) );
                assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                countOfNodes++;
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWork3()
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
                Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + lineCountInCSV + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label1 {uuid:row[0]})\n" + "RETURN currentnode;" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Node node = result.next().get( 0 ).asNode();
                assertTrue( node.hasLabel( "Label1" ) );
                assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                countOfNodes++;
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithLists() throws Exception
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
                Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label2 {uuid:row[0]})\n" + "RETURN [currentnode];" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Object> iterator = result.next().get( 0 ).asList().iterator();
                while ( iterator.hasNext() )
                {
                    Node node = (Node) iterator.next();
                    assertTrue( node.hasLabel( "Label2" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithListsOfLists() throws Exception
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
                Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label3 {uuid:row[0]})\n" + "RETURN [[currentnode]];" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Object> iterator = result.next().get( 0 ).asList().iterator();  // iterator over outer list
                assertTrue( iterator.hasNext() );
                iterator = ((List<Object>) iterator.next()).iterator();  // iterator over inner list
                while ( iterator.hasNext() )
                {
                    Node node = (Node) iterator.next();
                    assertTrue( node.hasLabel( "Label3" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithMaps() throws Exception
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
                Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label4 {uuid:row[0]})\n" + "RETURN {node:currentnode};" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Map.Entry<String,Object>> iterator = result.next().get( 0 ).asMap().entrySet().iterator();
                while ( iterator.hasNext() )
                {
                    Map.Entry<String,Object> entry = iterator.next();
                    assertEquals( "node", entry.getKey() );
                    Node node = (Node) entry.getValue();
                    assertTrue( node.hasLabel( "Label4" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithMapsWithinMaps() throws Exception
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
                Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label5 {uuid:row[0]})\n" + "RETURN {outer:{node:currentnode}};" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Map.Entry<String,Object>> iterator = result.next().get( 0 ).asMap().entrySet().iterator();
                assertTrue( iterator.hasNext() );
                iterator = ((Map<String,Object>) iterator.next().getValue()).entrySet().iterator();
                while ( iterator.hasNext() )
                {
                    Map.Entry<String,Object> entry = iterator.next();
                    assertEquals( "node", entry.getKey() );
                    Node node = (Node) entry.getValue();
                    assertTrue( node.hasLabel( "Label5" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }

            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void mixingPeriodicCommitAndLoadCSVShouldWorkWithMapsWithLists() throws Exception
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
                Session session = driver.session() )
        {
            StatementResult result = session.run(
                    "USING PERIODIC COMMIT " + (lineCountInCSV - 1) + "\n" + "LOAD CSV FROM \"" + url + "\" as row fieldterminator \" \"\n" +
                            "MERGE (currentnode:Label6 {uuid:row[0]})\n" + "RETURN {outer:[currentnode]};" );
            int countOfNodes = 0;
            while ( result.hasNext() )
            {
                Iterator<Map.Entry<String,Object>> mapIterator = result.next().get( 0 ).asMap().entrySet().iterator();
                assertTrue( mapIterator.hasNext() );
                Iterator<Object> iterator = ((List<Object>) mapIterator.next().getValue()).iterator();
                while ( iterator.hasNext() )
                {
                    Node node = (Node) iterator.next();
                    assertTrue( node.hasLabel( "Label6" ) );
                    assertEquals( String.valueOf( countOfNodes ), node.get( "uuid" ).asString() );
                    countOfNodes++;
                }
            }
            assertEquals( lineCountInCSV, countOfNodes );
        }
    }

    @Test
    public void shouldConsumeWithFailure()
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
              Session session = driver.session() )
        {
            String query = "UNWIND [1, 2, 3, 4, 0] AS x RETURN 10 / x";
            StatementResult result = session.run( query );

           try
           {
               result.consume();
               fail();
           }
           catch ( ClientException e )
           {
               assertTrue( e.code().contains( "ArithmeticError" ) );

               assertFalse( result.hasNext() );
               assertEquals( emptyList(), result.list() );

               ResultSummary summary = result.summary();
               assertEquals( query, summary.statement().text() );
           }
        }
    }

    @Test
    public void shouldFinishReadWriteQueryOnCancel()
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
              Session session = driver.session() )
        {
            String query = "UNWIND range(1, 2000) AS i CREATE (n) RETURN n";
            StatementResult result = session.run( query );

            result.consume(); // Without getting the result
        }
        // Open a new session and assert that 10 nodes got created
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
              Session session = driver.session() )
        {
            String query = "MATCH (n) RETURN count(n) AS c";
            StatementResult result = session.run( query );

            assertEquals( 2000, result.single().get( "c" ).asInt() );
        }
    }

    @Test
    public void shouldFinishWriteQueryOnCancel()
    {
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
              Session session = driver.session() )
        {
            String query = "UNWIND range(1, 2000) AS i CREATE (n)";
            StatementResult result = session.run( query );

            result.consume(); // Without getting the result
        }
        // Open a new session and assert that 10 nodes got created
        try ( Driver driver = graphDatabaseDriver( graphDb.boltURI() );
              Session session = driver.session() )
        {
            String query = "MATCH (n) RETURN count(n) AS c";
            StatementResult result = session.run( query );

            assertEquals( 2000, result.single().get( "c" ).asInt() );
        }
    }

    private URL prepareTestImportFile( int lines ) throws IOException
    {
        File tempFile = File.createTempFile( "testImport", ".csv" );
        try ( PrintWriter writer = FileUtils.newFilePrintWriter( tempFile, StandardCharsets.UTF_8 ) )
        {
            for ( int i = 0; i < lines; i++ )
            {
                writer.println( i + " " + i + " " + i );
            }
        }
        return tempFile.toURI().toURL();
    }
}
