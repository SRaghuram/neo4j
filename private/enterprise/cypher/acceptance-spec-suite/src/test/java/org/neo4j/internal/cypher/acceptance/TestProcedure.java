/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance;

import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_CONTINUE;
import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_PRUNE;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;
import static org.neo4j.internal.helpers.collection.Iterators.stream;
import static org.neo4j.procedure.Mode.WRITE;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class TestProcedure
{
    @Context
    public Transaction transaction;
    @Context
    public GraphDatabaseService db;
    @Context
    public DependencyResolver dependencyResolver;


    @Procedure( "org.neo4j.time" )
    @Description( "org.neo4j.time" )
    public void time( @Name( value = "time" ) LocalTime statementTime )
    {
        LocalTime realTime = LocalTime.now();
        Duration duration = Duration.between( statementTime, realTime );
    }

    @Procedure( "org.neo4j.aNodeWithLabel" )
    @Description( "org.neo4j.aNodeWithLabel" )
    public Stream<EntityResult> aNodeWithLabel( @Name( value = "label", defaultValue = "Dog" ) String label )
    {
        Result result = transaction.execute( "MATCH (n:" + label + ") RETURN n LIMIT 1" );
        return result.stream().map( row -> new EntityResult( (Node)row.get( "n" ) ) );
    }

    @Procedure( "org.neo4j.stream123" )
    @Description( "org.neo4j.stream123" )
    public Stream<CountResult> stream123()
    {
        return IntStream.of( 1, 2, 3 ).mapToObj( i -> new CountResult( i, "count" + i ) );
    }

    @Procedure( "org.neo4j.recurseN" )
    @Description( "org.neo4j.recurseN" )
    public Stream<EntityResult> recurseN( @Name( "n" ) Long n )
    {
        Result result;
        if ( n == 0 )
        {
            result = transaction.execute( "MATCH (n) RETURN n LIMIT 1" );
        }
        else
        {
            result = transaction.execute(
                    "UNWIND [1] AS i CALL org.neo4j.recurseN(" + (n - 1) + ") YIELD node RETURN node" );
        }
        return result.stream().map( row -> new EntityResult( (Node)row.get( "node" ) ) );
    }

    @Procedure( "org.neo4j.findNodesWithLabel" )
    @Description( "org.neo4j.findNodesWithLabel" )
    public Stream<EntityResult> findNodesWithLabel( @Name( "label" ) String label )
    {
        ResourceIterator<Node> nodes = transaction.findNodes( Label.label( label ) );
        return nodes.stream().map( EntityResult::new );
    }

    @Procedure( "org.neo4j.expandNode" )
    @Description( "org.neo4j.expandNode" )
    public Stream<EntityResult> expandNode( @Name( "nodeId" ) Long nodeId )
    {
        Node node = transaction.getNodeById( nodeId );
        List<Node> result = new ArrayList<>();
        for ( Relationship r : node.getRelationships() )
        {
            result.add( r.getOtherNode( node ) );
        }

        return result.stream().map( EntityResult::new );
    }

    @Procedure( name = "org.neo4j.createNodeWithLoop", mode = WRITE )
    @Description( "org.neo4j.createNodeWithLoop" )
    public Stream<EntityResult> createNodeWithLoop(
            @Name( "nodeLabel" ) String label, @Name( "relType" ) String relType )
    {
        Node node = transaction.createNode( Label.label( label ) );
        node.createRelationshipTo( node, RelationshipType.withName( relType ) );
        return Stream.of( new EntityResult( node ) );
    }

    @Procedure( name = "org.neo4j.graphAlgosDijkstra" )
    @Description( "org.neo4j.graphAlgosDijkstra" )
    public Stream<EntityResult> graphAlgosDijkstra(
            @Name( "startNode" ) Node start,
            @Name( "endNode" ) Node end,
            @Name( "relType" ) String relType,
            @Name( "weightProperty" ) String weightProperty )
    {
        var context = new BasicEvaluationContext( transaction, db );
        PathFinder<WeightedPath> pathFinder = GraphAlgoFactory.dijkstra( context,
                        PathExpanders.forTypeAndDirection(
                                RelationshipType.withName( relType ), Direction.BOTH ),
                        weightProperty );

        WeightedPath path = pathFinder.findSinglePath( start, end );
        return StreamSupport.stream( path.nodes().spliterator(), false ).map( EntityResult::new  );
    }

    @Procedure( name = "org.neo4j.setProperty", mode = WRITE )
    public Stream<EntityResult> setProperty( @Name( "node" ) Object entity, @Name( "propertyKey" ) String propertyKeyName, @Name( "value" ) String value )
    {
        if ( entity instanceof Node )
        {
            Node n = (Node) entity;
            if ( value == null )
            {
                n.removeProperty( propertyKeyName );
            }
            else
            {
                n.setProperty( propertyKeyName, value );
            }
            return Stream.of( new EntityResult( n ) );
        }
        else if ( entity instanceof Relationship )
        {
            Relationship r = (Relationship) entity;
            if ( value == null )
            {
                r.removeProperty( propertyKeyName );
            }
            else
            {
                r.setProperty( propertyKeyName, value );
            }
            return Stream.of( new EntityResult( r ) );
        }
        throw new RuntimeException( "Expected entity, but got " + entity );
    }

    @SuppressWarnings( "unchecked" )
    @UserFunction( name = "org.neo4j.toList" )
    public List<Object> toList( @Name( "value" ) Object value )
    {
        return (List<Object>)value;
    }

    @Procedure( name = "org.neo4j.matchNodeAndRelationship" )
    public Stream<EntityResult> matchNodeAndRelationship()
    {
        List<Map<String,Object>> result;
        try ( Transaction tx = db.beginTx() )
        {
            result = Iterators.asList( tx.execute( "MATCH (n)-[r]->() RETURN n AS node, r AS relationship" ) );
            tx.commit();
        }
        return result.stream().map( EntityResult::new );
    }

    @UserFunction( name = "org.neo4j.findByIdInTx" )
    public Node findByIdInTx( @Name( "id" ) Long id )
    {
        Node n;
        try ( Transaction tx = db.beginTx() )
        {
            n = tx.getNodeById( id );
            tx.commit();
        }
        return n;
    }

    @UserFunction( name = "org.neo4j.findById" )
    public Node findById( @Name( "id" ) Long id)
    {
        return transaction.getNodeById( id );
    }

    @UserFunction( name = "org.neo4j.findRelationshipById" )
    public Relationship findRelationshipById( @Name( "id" ) Long id)
    {
        return transaction.getRelationshipById( id );
    }

    // Only used for testing that query fails if procedure returns an entity from another database
    @UserFunction( name = "org.neo4j.findByIdInDatabase" )
    public Node findByIdInDatabase( @Name( "id" ) Long id, @Name("databaseName") String dbName, @Name("shouldCloseTransaction") Boolean shouldCloseTransaction )
    {
        GraphDatabaseService db = dependencyResolver.resolveDependency( DatabaseManagementService.class )
                                                    .database( dbName );
        Transaction tx = db.beginTx(2l, TimeUnit.SECONDS);
        Node n = tx.getNodeById( id );
        if (shouldCloseTransaction) {
            tx.commit();
        }

        return n;
    }

    // Only used for testing that query fails if procedure returns an entity from another database
    @UserFunction( name = "org.neo4j.findRelationshipByIdInDatabase" )
    public Relationship findRelationshipByIdInDatabase( @Name( "id" ) Long id, @Name("databaseName") String dbName, @Name("shouldCloseTransaction") Boolean shouldCloseTransaction )
    {
        GraphDatabaseService db = dependencyResolver.resolveDependency( DatabaseManagementService.class )
                                                    .database( dbName );
        Transaction tx = db.beginTx(2l, TimeUnit.SECONDS);
        Relationship r = tx.getRelationshipById( id );
        if (shouldCloseTransaction) {
            tx.commit();
        }

        return r;
    }

    // Only used for testing that query fails if procedure returns an entity from another database
    @UserFunction( name = "org.neo4j.findPropertyInDatabase" )
    public Object findPropertyInDatabase( @Name( "id" ) Long id, @Name("databaseName") String dbName, @Name("property") String property )
    {
        GraphDatabaseService db = dependencyResolver.resolveDependency( DatabaseManagementService.class )
                                                    .database( dbName );
        Transaction tx = db.beginTx();
        Node n = tx.getNodeById( id );
        Object prop = n.getProperty( property );
        tx.commit();

        return prop;
    }

    public static class EntityResult
    {
        public Node node;
        public Relationship relationship;

        EntityResult( Node node )
        {
            this.node = node;
        }

        EntityResult( Relationship relationship )
        {
            this.relationship = relationship;
        }

        EntityResult( Map<String, Object> map )
        {
            this.node = (Node)map.get( "node" );
            this.relationship = (Relationship)map.get( "relationship" );
        }
    }

    public static class RelationshipResult
    {
        public Relationship relationship;

        RelationshipResult( Relationship relationship )
        {
            this.relationship = relationship;
        }
    }

    public static class CountResult
    {
        public long count;
        public String name;

        CountResult( long count, String name )
        {
            this.count = count;
            this.name = name;
        }
    }

    @Procedure( "org.neo4j.movieTraversal" )
    @Description( "org.neo4j.movieTraversal" )
    public Stream<PathResult> movieTraversal( @Name( "start" ) Node start )
    {
        TraversalDescription td =
                transaction.traversalDescription()
                        .breadthFirst()
                        .relationships( RelationshipType.withName( "ACTED_IN" ), Direction.BOTH )
                        .relationships( RelationshipType.withName( "PRODUCED" ), Direction.BOTH )
                        .relationships( RelationshipType.withName( "DIRECTED" ), Direction.BOTH )
                        .evaluator( Evaluators.fromDepth( 3 ) )
                        .evaluator( new LabelEvaluator( "Western", 1, 3 ) )
                        .uniqueness( Uniqueness.NODE_GLOBAL );

        return stream( td.traverse( start ).iterator() ).map( PathResult::new );
    }

    @Procedure( "org.neo4j.internalTypes" )
    public Stream<InternalTypeResult> internal( @Name( value = "text", defaultValue = "Dog" ) TextValue text,
            @Name( value = "map", defaultValue = "{key: 1337}" ) MapValue map )
    {
        return Stream.of( new InternalTypeResult( text, map ) );
    }

    public static class InternalTypeResult
    {
        public final TextValue textValue;
        public final MapValue mapValue;

        public InternalTypeResult( TextValue textValue, MapValue mapValue )
        {
            this.textValue = textValue;
            this.mapValue = mapValue;
        }
    }

    public static class PathResult
    {
        public Path path;

        PathResult( Path path )
        {
            this.path = path;
        }
    }

    public static class LabelEvaluator implements Evaluator
    {
        private Set<String> endNodeLabels;
        private long limit;
        private long minLevel;
        private long resultCount;

        LabelEvaluator( String endNodeLabel, long limit, int minLevel )
        {
            this.limit = limit;
            this.minLevel = minLevel;

            endNodeLabels = Collections.singleton( endNodeLabel );
        }

        @Override
        public Evaluation evaluate( Path path )
        {
            int depth = path.length();
            Node check = path.endNode();

            if ( depth < minLevel )
            {
                return EXCLUDE_AND_CONTINUE;
            }

            if ( limit != -1 && resultCount >= limit )
            {
                return EXCLUDE_AND_PRUNE;
            }

            return labelExists( check, endNodeLabels ) ? countIncludeAndContinue() : EXCLUDE_AND_CONTINUE;
        }

        private boolean labelExists( Node node, Set<String> labels )
        {
            if ( labels.isEmpty() )
            {
                return false;
            }

            for ( Label lab : node.getLabels() )
            {
                if ( labels.contains( lab.name() ) )
                {
                    return true;
                }
            }
            return false;
        }

        private Evaluation countIncludeAndContinue()
        {
            resultCount++;
            return INCLUDE_AND_CONTINUE;
        }
    }
}
