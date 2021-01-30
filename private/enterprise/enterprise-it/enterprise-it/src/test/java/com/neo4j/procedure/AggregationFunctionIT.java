/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.jar.JarBuilder;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

@TestDirectoryExtension
class AggregationFunctionIT
{
    @Inject
    private TestDirectory plugins;

    private GraphDatabaseService db;
    private DatabaseManagementService managementService;

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleSingleStringArgumentAggregationFunction( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE ({ prop:'foo'})" );
            tx.execute( "CREATE ({ prop:'foo'})" );
            tx.execute( "CREATE ({ prop:'bar'})" );
            tx.execute( "CREATE ({prop:'baz'})" );
            tx.execute( "CREATE ()" );
            tx.commit();
        }

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            Result result = transaction.execute( format( "CYPHER runtime=%s MATCH (n) RETURN com.neo4j.procedure.count(n.prop) AS count", runtime ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "count", 4L ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleSingleStringArgumentAggregationFunctionAndGroupingKey( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE ({prop1:42, prop2:'foo'})" );
            tx.execute( "CREATE ({prop1:42, prop2:'foo'})" );
            tx.execute( "CREATE ({prop1:42, prop2:'bar'})" );
            tx.execute( "CREATE ({prop1:1337, prop2:'baz'})" );
            tx.execute( "CREATE ({prop1:1337})" );
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            // When
            Result result = transaction.execute( format( "CYPHER runtime=%s MATCH (n) RETURN n.prop1, com.neo4j.procedure.count(n.prop2) AS count", runtime ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "n.prop1", 42L, "count", 3L ) );
            assertThat( result.next() ).isEqualTo( map( "n.prop1", 1337L, "count", 1L ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldFailNicelyWhenInvalidRuntimeType( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE ({ prop:'foo'})" );
            tx.execute( "CREATE ({ prop:'foo'})" );
            tx.execute( "CREATE ({ prop:'bar'})" );
            tx.execute( "CREATE ({prop:42})" );
            tx.execute( "CREATE ()" );
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            QueryExecutionException exception =
                    assertThrows( QueryExecutionException.class,
                                  () -> transaction
                                          .execute( format( "CYPHER runtime=%s MATCH (n) RETURN com.neo4j.procedure.count(n.prop) AS count", runtime ) )
                                          .resultAsString() );
            assertThat( exception.getMessage() ).contains( "Can't coerce `Long(42)` to String" );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleNodeArgumentAggregationFunction( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE ({ level:42})" );
            tx.execute( "CREATE ({ level:1337})" );
            tx.execute( "CREATE ({ level:0})" );
            tx.execute( "CREATE ()" );
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            // When
            Result result = transaction
                    .execute( format( "CYPHER runtime=%s MATCH (n) WITH com.neo4j.procedure.findBestNode(n) AS best RETURN best.level AS level", runtime ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "level", 1337L ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleRelationshipArgumentAggregationFunction( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE ()-[:T {level:42}]->()" );
            tx.execute( "CREATE ()-[:T {level:1337}]->()" );
            tx.execute( "CREATE ()-[:T {level:2}]->()" );
            tx.execute( "CREATE ()-[:T]->()" );
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            // When
            Result result = transaction.execute(
                    format( "CYPHER runtime=%s MATCH ()-[r]->() WITH com.neo4j.procedure.findBestRel(r) AS best RETURN best.level AS level", runtime ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "level", 1337L ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandlePathArgumentAggregationFunction( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE ()-[:T]->()" );
            tx.execute( "CREATE ()-[:T]->()-[:T]->()" );
            tx.execute( "CREATE ()-[:T]->()-[:T]->()-[:T]->()" );
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            // When
            Result result = transaction.execute(
                    format( "CYPHER runtime=%s MATCH p=()-[:T*]->() WITH com.neo4j.procedure.longestPath(p) AS longest RETURN length(longest) AS len",
                            runtime ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "len", 3L ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleNullPath( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // When
            Result result = transaction
                    .execute( format( "CYPHER runtime=%s MATCH p=()-[:T*]->() WITH com.neo4j.procedure.longestPath(p) AS longest RETURN longest", runtime ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "longest", null ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleNumberArgumentAggregationFunction( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given, When
            Result result = transaction
                    .execute( format( "CYPHER runtime=%s UNWIND [43, 42.5, 41.9, 1337] AS num RETURN com.neo4j.procedure.near42(num) AS closest", runtime ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "closest", 41.9 ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleDoubleArgumentAggregationFunction( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given, When
            Result result = transaction.execute(
                    format( "CYPHER runtime=%s UNWIND [43, 42.5, 41.9, 1337] AS num RETURN com.neo4j.procedure.doubleAggregator(num) AS closest", runtime ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "closest", 41.9 ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleLongArgumentAggregationFunction( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given, When
            Result result = transaction.execute(
                    format( "CYPHER runtime=%s UNWIND [43, 42.5, 41.9, 1337] AS num RETURN com.neo4j.procedure.longAggregator(num) AS closest", runtime ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "closest", 42L ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleNoArgumentBooleanAggregationFunction( String runtime )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat(
                    transaction.execute( format( "CYPHER runtime=%s UNWIND [1,2] AS num RETURN com.neo4j.procedure.boolAggregator() AS wasCalled", runtime ) )
                               .next() )
                    .isEqualTo( map( "wasCalled", true ) );
            assertThat( transaction.execute( format( "CYPHER runtime=%s UNWIND [] AS num RETURN com.neo4j.procedure.boolAggregator() AS wasCalled", runtime ) )
                                   .next() )
                    .isEqualTo( map( "wasCalled", false ) );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldBeAbleToUseAdbInFunction( String runtime )
    {
        List<Node> nodes = new ArrayList<>();
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            nodes.add( tx.createNode() );
            nodes.add( tx.createNode() );
            nodes.add( tx.createNode() );
            nodes.add( tx.createNode() );
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            // When
            Result result = transaction
                    .execute( format( "CYPHER runtime=%s UNWIND $ids AS ids WITH com.neo4j.procedure.collectNode(ids) AS nodes RETURN nodes", runtime ),
                              map( "ids", nodes.stream().map( Node::getId ).collect( Collectors.toList() ) ) );

            // Then
            assertThat( result.next() ).isEqualTo( map( "nodes", nodes ) );
            assertFalse( result.hasNext() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldBeAbleToAccessPropertiesFromAggregatedValues( String runtime )
    {
        // Given
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE (:User {country: 'Sweden'})" );
            tx.execute( "CREATE (:User {country: 'Sweden'})" );
            tx.execute( "CREATE (:User {country: 'Sweden'})" );
            tx.execute( "CREATE (:User {country: 'Sweden'})" );
            tx.execute( "CREATE (:User {country: 'Germany'})" );
            tx.execute( "CREATE (:User {country: 'Germany'})" );
            tx.execute( "CREATE (:User {country: 'Germany'})" );
            tx.execute( "CREATE (:User {country: 'Mexico'})" );
            tx.execute( "CREATE (:User {country: 'Mexico'})" );
            tx.execute( "CREATE (:User {country: 'South Korea'})" );
            tx.commit();
        }

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            List<Map<String,Object>> result =
                    Iterators.asList( transaction.execute(
                            format( "CYPHER runtime=%s MATCH (u:User) RETURN u.country,count(*),com.neo4j.procedure.first(u).country AS first", runtime ) ) );

            // Then
            assertThat( result ).hasSize( 4 );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void shouldHandleDefaultArgument( String runtime )
    {
        // Given, empty db

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            Map<String,Object> result =
                    Iterators.single( transaction.execute(
                            format( "CYPHER runtime=%s UNWIND range(1,100) AS i RETURN com.neo4j.procedure.count() AS count", "PIPELINED" ) ) );

            // Then
            assertThat( result ).isEqualTo( Map.of( "count", 100L ) );
            transaction.commit();
        }
    }

    @BeforeEach
    void setUp() throws IOException
    {
        new JarBuilder().createJarFor( plugins.createFile( "myFunctions.jar" ), ClassWithFunctions.class );
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder().impermanent()
                                                                                .setConfig( GraphDatabaseSettings.plugin_dir, plugins.absolutePath() )
                                                                                .setConfig( OnlineBackupSettings.online_backup_enabled, false ).build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        if ( this.db != null )
        {
            this.managementService.shutdown();
        }
    }

    public static class ClassWithFunctions
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Transaction transaction;

        @Context
        public Log log;

        @UserAggregationFunction
        public CountAggregator count()
        {
            return new CountAggregator();
        }

        @UserAggregationFunction
        public RelAggregator findBestRel()
        {
            return new RelAggregator();
        }

        @UserAggregationFunction
        public LongestPathAggregator longestPath()
        {
            return new LongestPathAggregator();
        }

        @UserAggregationFunction
        public NodeAggregator findBestNode()
        {
            return new NodeAggregator();
        }

        @UserAggregationFunction
        public DoubleAggregator doubleAggregator()
        {
            return new DoubleAggregator();
        }

        @UserAggregationFunction
        public LongAggregator longAggregator()
        {
            return new LongAggregator();
        }

        @UserAggregationFunction
        public BoolAggregator boolAggregator()
        {
            return new BoolAggregator();
        }

        @UserAggregationFunction
        public ClosestTo42Aggregator near42()
        {
            return new ClosestTo42Aggregator();
        }

        @UserAggregationFunction
        public NodeFromIdAggregator collectNode()
        {
            return new NodeFromIdAggregator( transaction );
        }

        @UserAggregationFunction
        public FirstAggregator first()
        {
            return new FirstAggregator();
        }

        public static class FirstAggregator
        {
            private Object first;

            @UserAggregationUpdate
            public void update( @Name( "item" ) Object o )
            {
                if ( first == null )
                {
                    first = o;
                }
            }

            @UserAggregationResult
            public Object result()
            {
                return first;
            }
        }

        public static class NodeAggregator
        {
            private Node aggregateNode;

            @UserAggregationUpdate
            public void update( @Name( "node" ) Node node )
            {
                if ( node != null )
                {
                    long level = (long) node.getProperty( "level", 0L );

                    if ( aggregateNode == null )
                    {
                        aggregateNode = node;
                    }
                    else if ( level > (long) aggregateNode.getProperty( "level", 0L ) )
                    {
                        aggregateNode = node;
                    }
                }
            }

            @UserAggregationResult
            public Node result()
            {
                return aggregateNode;
            }
        }

        public static class RelAggregator
        {
            private Relationship aggregateRel;

            @UserAggregationUpdate
            public void update( @Name( "rel" ) Relationship rel )
            {
                if ( rel != null )
                {
                    long level = (long) rel.getProperty( "level", 0L );

                    if ( aggregateRel == null )
                    {
                        aggregateRel = rel;
                    }
                    else if ( level > (long) aggregateRel.getProperty( "level", 0L ) )
                    {
                        aggregateRel = rel;
                    }
                }
            }

            @UserAggregationResult
            public Relationship result()
            {
                return aggregateRel;
            }
        }

        public static class LongestPathAggregator
        {
            private Path aggregatePath;
            private int longest;

            @UserAggregationUpdate
            public void update( @Name( "path" ) Path path )
            {
                if ( path != null )
                {
                    if ( path.length() > longest )
                    {
                        longest = path.length();
                        aggregatePath = path;
                    }
                }
            }

            @UserAggregationResult
            public Path result()
            {
                return aggregatePath;
            }
        }

        public static class ClosestTo42Aggregator
        {
            private Number closest;

            @UserAggregationUpdate
            public void update( @Name( "number" ) Number number )
            {
                if ( number != null )
                {
                    if ( closest == null )
                    {
                        closest = number;
                    }
                    else if ( Math.abs( number.doubleValue() - 42L ) < Math.abs( closest.doubleValue() - 42L ) )
                    {
                        closest = number;
                    }
                }
            }

            @UserAggregationResult
            public Number result()
            {
                return closest;
            }
        }

        public static class DoubleAggregator
        {
            private Double closest;

            @UserAggregationUpdate
            public void update( @Name( "double" ) Double number )
            {
                if ( number != null )
                {
                    if ( closest == null )
                    {
                        closest = number;
                    }
                    else if ( Math.abs( number - 42L ) < Math.abs( closest - 42L ) )
                    {
                        closest = number;
                    }
                }
            }

            @UserAggregationResult
            public Double result()
            {
                return closest;
            }
        }

        public static class LongAggregator
        {
            private Long closest;

            @UserAggregationUpdate
            public void update( @Name( "long" ) Long number )
            {
                if ( number != null )
                {
                    if ( closest == null )
                    {
                        closest = number;
                    }
                    else if ( Math.abs( number - 42L ) < Math.abs( closest - 42L ) )
                    {
                        closest = number;
                    }
                }
            }

            @UserAggregationResult
            public Long result()
            {
                return closest;
            }
        }

        public static class CountAggregator
        {
            private long count;

            @UserAggregationUpdate
            public void update( @Name( value = "in", defaultValue = "hello" ) String in )
            {
                if ( in != null )
                {
                    count += 1L;
                }
            }

            @UserAggregationResult
            public long result()
            {
                return count;
            }
        }

        public static class BoolAggregator
        {
            private boolean wasCalled;

            @UserAggregationUpdate
            public void update()
            {
                wasCalled = true;
            }

            @UserAggregationResult
            public boolean result()
            {
                return wasCalled;
            }
        }

        public static class NodeFromIdAggregator
        {
            private final List<Long> ids = new ArrayList<>();
            private final Transaction transaction;

            NodeFromIdAggregator( Transaction transaction )
            {
                this.transaction = transaction;
            }

            @UserAggregationUpdate
            public void update( @Name( "id" ) long id )
            {
                ids.add( id );
            }

            @UserAggregationResult
            public List<Node> result()
            {
                return ids.stream().map( transaction::getNodeById ).collect( Collectors.toList() );
            }
        }
    }
}
