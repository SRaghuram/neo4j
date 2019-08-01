/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.test.rule.SuppressOutput;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.Assert.assertThat;

/**
 * We need to ensure that failures that come out of our read-committed isolation level, are turned into "transient" exceptions from the driver,
 * such that clients are instructed to retry their transactions when such conflicts arise.
 */
public class ReadAndDeleteTransactionConflictIT
{
    private SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private RepeatRule outerRepeat = new RepeatRule( false, 1 );
    private Neo4jRule graphDb = new Neo4jRule()
            .dumpLogsOnFailure( () -> System.err ); // Late-bind to System.err to work better with SuppressOutput rule.
    private CleanupRule cleanupRule = new CleanupRule();
    private RepeatRule innerRepeat = new RepeatRule( true, 10 );

    private Driver driver;

    @Rule
    public RuleChain rules = RuleChain.outerRule( suppressOutput ).around( outerRepeat ).around( graphDb ).around( cleanupRule ).around( innerRepeat );

    @Test
    public void relationshipsThatAreConcurrentlyDeletedWhileStreamingResultThroughBoltMustBeIgnored()
    {
        Driver driver = getDriver();
        try ( Session readSession = driver.session();
              Session writeSession = driver.session() )
        {
            StatementResult result = writeSession.run(
                    "create (n) with n unwind range(1, 1000) as x create (n)-[:REL]->(n)" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated(), is( 1 ) );
            assertThat( counters.relationshipsCreated(), is( 1000 ) );

            int relCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                StatementResult readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match ()-[r]->() return 1 as whatever, r" );
                    StatementResult deleteResult = deleter.run( "match (n) detach delete n" );
                    deleteResult.consume();
                    deleter.success();
                }

                while ( readResult.hasNext() )
                {
                    Record record = readResult.next();
                    Value value = record.get( "r" );
                    relCounter++;
                    assertThat( value.asRelationship().type(), is( "REL" ) );
                }
            }
            assertThat( relCounter, is( lessThanOrEqualTo( 1000 ) ) );
        }
        catch ( TransientException ignore )
        {
            // Getting a transient exception is allowed, because that just signals to clients that their transaction conflicted, and should be retried.
        }
    }

    @Test
    public void relationshipsWithPropertiesThatAreConcurrentlyDeletedWhileStreamingResultThroughBoltMustBeIgnored()
    {
        Driver driver = getDriver();
        try ( Session readSession = driver.session();
              Session writeSession = driver.session() )
        {
            StatementResult result = writeSession.run(
                    "create (n) with n unwind range(1, 1000) as x create (n)-[:REL {a: 1}]->(n)" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated(), is( 1 ) );
            assertThat( counters.relationshipsCreated(), is( 1000 ) );

            int relCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                StatementResult readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match ()-[r]->() return 1 as whatever, r" );
                    StatementResult deleteResult = deleter.run( "match (n) detach delete n" );
                    deleteResult.consume();
                    deleter.success();
                }

                while ( readResult.hasNext() )
                {
                    Record record = readResult.next();
                    Value value = record.get( "r" );
                    relCounter++;
                    assertThat( value.asRelationship().asMap().get( "a" ), is( oneOf(  1L, null ) ) );
                }
            }
            assertThat( relCounter, is( lessThanOrEqualTo( 1000 ) ) );
        }
        catch ( TransientException ignore )
        {
            // Getting a transient exception is allowed, because that just signals to clients that their transaction conflicted, and should be retried.
        }
    }

    @Test
    public void nodesThatAreConcurrentlyDeletedWhileStreamingResultThroughBoltMustBeIgnored()
    {
        Driver driver = getDriver();
        try ( Session readSession = driver.session();
              Session writeSession = driver.session() )
        {
            StatementResult result = writeSession.run(
                    "unwind range(1, 1000) as x create (n:A:B:C:D:E:F:G:H:I:J:K:L:O:P:Q)" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated(), is( 1000 ) );
            assertThat( counters.relationshipsCreated(), is( 0 ) );

            int nodeCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                StatementResult readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (n) return 1 as whatever, n" );
                    StatementResult deleteResult = deleter.run( "match (n) delete n" );
                    deleteResult.consume();
                    deleter.success();
                }

                while ( readResult.hasNext() )
                {
                    Record record = readResult.next();
                    Value value = record.get( "n" );
                    nodeCounter++;
                    assertThat( value.asNode(), notNullValue() );
                }
            }
            assertThat( nodeCounter, is( lessThanOrEqualTo( 1000 ) ) );
        }
        catch ( TransientException ignore )
        {
            // Getting a transient exception is allowed, because that just signals to clients that their transaction conflicted, and should be retried.
        }
    }

    @Test
    public void nodesWithPropertiesThatAreConcurrentlyDeletedWhileStreamingResultThroughBoltMustBeIgnored()
    {
        Driver driver = getDriver();
        try ( Session readSession = driver.session();
              Session writeSession = driver.session() )
        {
            StatementResult result = writeSession.run(
                    "unwind range(1, 1000) as x create (n {a: 1})" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated(), is( 1000 ) );
            assertThat( counters.relationshipsCreated(), is( 0 ) );

            int nodeCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                StatementResult readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (n) return 1 as whatever, n" );
                    StatementResult deleteResult = deleter.run( "match (n) delete n" );
                    deleteResult.consume();
                    deleter.success();
                }

                while ( readResult.hasNext() )
                {
                    Record record = readResult.next();
                    Value value = record.get( "n" );
                    nodeCounter++;
                    assertThat( value.asNode().asMap().get( "a" ), is( oneOf( 1L, null ) ) );
                }
            }
            assertThat( nodeCounter, is( lessThanOrEqualTo( 1000 ) ) );
        }
        catch ( TransientException ignore )
        {
            // Getting a transient exception is allowed, because that just signals to clients that their transaction conflicted, and should be retried.
        }
    }

    private Driver getDriver()
    {
        if ( driver == null )
        {
            driver = graphDatabaseDriver( graphDb.boltURI() );
            cleanupRule.add( driver );
            cleanupRule.add( () ->
            {
                // Clear the driver field when the driver is closed, to ensure we will create a new driver in the next iteration.
                driver = null;
            } );

        }
        return driver;
    }
}
