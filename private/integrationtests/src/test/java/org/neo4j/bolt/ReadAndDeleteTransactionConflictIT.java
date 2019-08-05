/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.bolt;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * We need to ensure that failures that come out of our read-committed isolation level, are turned into "transient" exceptions from the driver,
 * such that clients are instructed to retry their transactions when such conflicts arise.
 */
public class ReadAndDeleteTransactionConflictIT
{
    private static SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private static Neo4jRule graphDb = new Neo4jRule()
            .dumpLogsOnFailure( () -> System.err ); // Late-bind to System.err to work better with SuppressOutput rule.
    private static CleanupRule cleanupRule = new CleanupRule();

    private static Driver driver;

    @ClassRule
    public static RuleChain rules = RuleChain.outerRule( suppressOutput ).around( graphDb ).around( cleanupRule );

    @BeforeClass
    public static void setUp()
    {
        Config config = Config.build().withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig();
        driver = GraphDatabase.driver( graphDb.boltURI(), config );
        cleanupRule.add( driver );
    }

    @Test
    public void relationshipsThatAreConcurrentlyDeletedWhileStreamingResultThroughBoltMustBeIgnored()
    {
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
                    assertThat( value.asRelationship().asMap().get( "a" ), isOneOf( 1L, null ) );
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
                    assertThat( value.asNode().asMap().get( "a" ), isOneOf( 1L, null ) );
                }
            }
            assertThat( nodeCounter, is( lessThanOrEqualTo( 1000 ) ) );
        }
        catch ( TransientException ignore )
        {
            // Getting a transient exception is allowed, because that just signals to clients that their transaction conflicted, and should be retried.
        }
    }
}
