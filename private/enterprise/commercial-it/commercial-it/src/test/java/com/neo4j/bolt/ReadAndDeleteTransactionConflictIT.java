/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.StatementResult;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.SuppressOutput;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.hamcrest.Matchers.equalTo;
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
    private static final SuppressOutput suppressOutput = SuppressOutput.suppress();
    private static final Neo4jRule graphDb = new Neo4jRule()
            .dumpLogsOnFailure( () -> System.err ); // Late-bind to System.err to work better with SuppressOutput rule.
    private static final CleanupRule cleanupRule = new CleanupRule();

    private static Driver driver;

    @ClassRule
    public static final RuleChain rules = RuleChain.outerRule( suppressOutput ).around( graphDb ).around( cleanupRule );

    @BeforeClass
    public static void setUp()
    {
        driver = graphDatabaseDriver( graphDb.boltURI() );
        cleanupRule.add( driver );
    }

    @Test
    public void returningNodesDeletedInSameTransactionMustReturnEmptyNodes()
    {
        // It is weird that we are returning these empty nodes, but this test is just codifying the current behaviour.
        // In the future, deleted entities will behave as if they are NULLs.
        // See CIP2018-10-19 for the details of these plans: https://github.com/opencypher/openCypher/pull/332
        try ( Session session = driver.session() )
        {
            Value nodeId = session.run( "create (n:L1 {a: 'b'}) return id(n)" ).single().get( 0 );
            Record record = session.run( "match (n:L1) where id(n) = $nodeId delete n return n", Values.parameters( "nodeId", nodeId ) ).single();
            Map<String,Object> map = record.get( 0 ).asMap();
            assertThat( map, equalTo( new HashMap<>() ) );
        }
    }

    @Test
    public void returningRelationshipsDeletedInSameTransactionMustEmptyRelationships()
    {
        try ( Session session = driver.session() )
        {
            Value nodeId = session.run( "create (n:L2)-[:REL]->(m) return id(n)" ).single().get( 0 );
            StatementResult result = session.run( "match (n:L2)-[r]->(m) where id(n) = $nodeId delete n, m, r return r",
                    Values.parameters( "nodeId", nodeId ) );
            Record record = result.single();
            Map<String,Object> map = record.get( 0 ).asMap();
            assertThat( map, equalTo( new HashMap<>() ) );
        }
    }

    @Test
    public void returningRelationshipPropertiesOfRelationshipDeletedInSameTransactionMustNotThrow()
    {
        try ( Session session = driver.session() )
        {
            Value nodeId = session.run( "create (n:L3)-[:REL {a: 1}]->(m) return id(n)" ).single().get( 0 );
            StatementResult result = session.run( "" +
                    "match (n:L3)-[r]->(m) " +
                    "where id(n) = $nodeId " +
                    "with n, m, r, properties(r) as props " +
                    "delete n, m, r " +
                    "return props", Values.parameters( "nodeId", nodeId ) );
            long value = result.single().get( 0 ).get( "a" ).asLong();
            assertThat( value, equalTo( 1L ) );
        }
    }

    @Test
    public void relationshipsThatAreConcurrentlyDeletedWhileStreamingResultThroughBoltMustBeIgnored()
    {
        try ( Session readSession = driver.session();
              Session writeSession = driver.session() )
        {
            StatementResult result = writeSession.run(
                    "create (n:L4) with n unwind range(1, 1000) as x create (n)-[:REL]->(n)" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated(), is( 1 ) );
            assertThat( counters.relationshipsCreated(), is( 1000 ) );

            int relCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                StatementResult readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (:L4)-[r]->() return 1 as whatever, r" );
                    StatementResult deleteResult = deleter.run( "match (n:L4) detach delete n" );
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
                    "create (n:L5) with n unwind range(1, 1000) as x create (n)-[:REL {a: 1}]->(n)" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated(), is( 1 ) );
            assertThat( counters.relationshipsCreated(), is( 1000 ) );

            int relCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                StatementResult readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (:L5)-[r]->() return 1 as whatever, r" );
                    StatementResult deleteResult = deleter.run( "match (n:L5) detach delete n" );
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
        try ( Session readSession = driver.session();
              Session writeSession = driver.session() )
        {
            StatementResult result = writeSession.run(
                    "unwind range(1, 1000) as x create (n:L6:A:B:C:D:E:F:G:H:I:J:K:L:O:P:Q)" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated(), is( 1000 ) );
            assertThat( counters.relationshipsCreated(), is( 0 ) );

            int nodeCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                StatementResult readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (n:L6) return 1 as whatever, n" );
                    StatementResult deleteResult = deleter.run( "match (n:L6) delete n" );
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
                    "unwind range(1, 1000) as x create (n:L7 {a: 1})" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated(), is( 1000 ) );
            assertThat( counters.relationshipsCreated(), is( 0 ) );

            int nodeCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                StatementResult readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (n:L7) return 1 as whatever, n" );
                    StatementResult deleteResult = deleter.run( "match (n:L7) delete n" );
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
}
