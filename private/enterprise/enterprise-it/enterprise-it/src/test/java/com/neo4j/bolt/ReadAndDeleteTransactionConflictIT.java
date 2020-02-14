/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.SuppressOutput;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.assertj.core.api.Assertions.assertThat;

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
            assertThat( map ).isEqualTo( new HashMap<>() );
        }
    }

    @Test
    public void returningRelationshipsDeletedInSameTransactionMustEmptyRelationships()
    {
        try ( Session session = driver.session() )
        {
            Value nodeId = session.run( "create (n:L2)-[:REL]->(m) return id(n)" ).single().get( 0 );
            Result result = session.run( "match (n:L2)-[r]->(m) where id(n) = $nodeId delete n, m, r return r",
                    Values.parameters( "nodeId", nodeId ) );
            Record record = result.single();
            Map<String,Object> map = record.get( 0 ).asMap();
            assertThat( map ).isEqualTo( new HashMap<>() );
        }
    }

    @Test
    public void returningRelationshipPropertiesOfRelationshipDeletedInSameTransactionMustNotThrow()
    {
        try ( Session session = driver.session() )
        {
            Value nodeId = session.run( "create (n:L3)-[:REL {a: 1}]->(m) return id(n)" ).single().get( 0 );
            Result result = session.run( "" +
                    "match (n:L3)-[r]->(m) " +
                    "where id(n) = $nodeId " +
                    "with n, m, r, properties(r) as props " +
                    "delete n, m, r " +
                    "return props", Values.parameters( "nodeId", nodeId ) );
            long value = result.single().get( 0 ).get( "a" ).asLong();
            assertThat( value ).isEqualTo( 1L );
        }
    }

    @Test
    public void relationshipsThatAreConcurrentlyDeletedWhileStreamingResultThroughBoltMustBeIgnored()
    {
        try ( Session readSession = driver.session();
              Session writeSession = driver.session() )
        {
            Result result = writeSession.run(
                    "create (n:L4) with n unwind range(1, 1000) as x create (n)-[:REL]->(n)" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated() ).isEqualTo( 1 );
            assertThat( counters.relationshipsCreated() ).isEqualTo( 1000 );

            int relCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                Result readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (:L4)-[r]->() return 1 as whatever, r" );
                    Result deleteResult = deleter.run( "match (n:L4) detach delete n" );
                    deleteResult.consume();
                    deleter.commit();
                }

                while ( readResult.hasNext() )
                {
                    Record record = readResult.next();
                    Value value = record.get( "r" );
                    relCounter++;
                    assertThat( value.asRelationship().type() ).isEqualTo( "REL" );
                }
            }
            assertThat( relCounter ).isLessThanOrEqualTo( 1000 );
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
            Result result = writeSession.run(
                    "create (n:L5) with n unwind range(1, 1000) as x create (n)-[:REL {a: 1}]->(n)" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated() ).isEqualTo( 1 );
            assertThat( counters.relationshipsCreated() ).isEqualTo( 1000 );

            int relCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                Result readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (:L5)-[r]->() return 1 as whatever, r" );
                    Result deleteResult = deleter.run( "match (n:L5) detach delete n" );
                    deleteResult.consume();
                    deleter.commit();
                }

                while ( readResult.hasNext() )
                {
                    Record record = readResult.next();
                    Value value = record.get( "r" );
                    relCounter++;
                    assertThat( value.asRelationship().asMap().get( "a" ) ).isIn(1L, null );
                }
            }
            assertThat( relCounter ).isLessThanOrEqualTo( 1000 );
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
            Result result = writeSession.run(
                    "unwind range(1, 1000) as x create (n:L6:A:B:C:D:E:F:G:H:I:J:K:L:O:P:Q)" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated() ).isEqualTo( 1000 );
            assertThat( counters.relationshipsCreated() ).isEqualTo( 0 );

            int nodeCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                Result readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (n:L6) return 1 as whatever, n" );
                    Result deleteResult = deleter.run( "match (n:L6) delete n" );
                    deleteResult.consume();
                    deleter.commit();
                }

                while ( readResult.hasNext() )
                {
                    Record record = readResult.next();
                    Value value = record.get( "n" );
                    nodeCounter++;
                    assertThat( value.asNode() ).isNotNull();
                }
            }
            assertThat( nodeCounter ).isLessThanOrEqualTo( 1000 );
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
            Result result = writeSession.run(
                    "unwind range(1, 1000) as x create (n:L7 {a: 1})" );
            SummaryCounters counters = result.consume().counters();
            assertThat( counters.nodesCreated() ).isEqualTo( 1000 );
            assertThat( counters.relationshipsCreated() ).isEqualTo( 0 );

            int nodeCounter = 0;
            try ( Transaction reader = readSession.beginTransaction() )
            {
                Result readResult;
                try ( Transaction deleter = writeSession.beginTransaction() )
                {
                    readResult = reader.run( "match (n:L7) return 1 as whatever, n" );
                    Result deleteResult = deleter.run( "match (n:L7) delete n" );
                    deleteResult.consume();
                    deleter.commit();
                }

                while ( readResult.hasNext() )
                {
                    Record record = readResult.next();
                    Value value = record.get( "n" );
                    nodeCounter++;
                    assertThat( value.asNode().asMap().get( "a" ) ).isIn( 1L, null );
                }
            }
            assertThat( nodeCounter ).isLessThanOrEqualTo( 1000 );
        }
        catch ( TransientException ignore )
        {
            // Getting a transient exception is allowed, because that just signals to clients that their transaction conflicted, and should be retried.
        }
    }
}
