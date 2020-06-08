/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.migration;

import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;

import java.nio.file.Path;
<<<<<<< HEAD
=======
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
<<<<<<< HEAD
=======
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
<<<<<<< HEAD
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;

import static java.lang.Math.toIntExact;
import static org.neo4j.configuration.GraphDatabaseSettings.CheckpointPolicy.CONTINUOUS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
=======
import org.neo4j.internal.freki.FrekiStorageEngine;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static org.neo4j.configuration.GraphDatabaseSettings.CheckpointPolicy.CONTINUOUS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.util.Preconditions.checkArgument;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

public class MigrateDataIntoOtherDatabase
{
    public static void main( String[] args )
    {
        Path fromHome = Path.of( args[0] );
        Path toHome = Path.of( args[1] );
<<<<<<< HEAD
        migrate( fromHome, toHome );
    }

    public static void migrate( Path fromHome, Path toHome )
    {
        assert !(fromHome.equals( toHome ));
        System.out.println( String.format( "Migrating %s to Freki store: %s", fromHome, toHome ) );
=======
        migrate( fromHome, toHome, true, true );
    }

    public static void migrate( Path fromHome, Path toHome, boolean validate, boolean statistics )
    {
        checkArgument( !fromHome.equals( toHome ), "From directory same as to: %s", fromHome );
        System.out.println( format( "Migrating %s to Freki store: %s", fromHome, toHome ) );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        long totalPageCacheMem = ConfiguringPageCacheFactory.defaultHeuristicPageCacheMemory();
        String pcMemory = String.valueOf( totalPageCacheMem / 10 );
        System.out.println( "Using a pagecache of size " + pcMemory );
        DatabaseManagementService fromDbms = new EnterpriseDatabaseManagementServiceBuilder( fromHome.toFile() )
                .setConfig( GraphDatabaseSettings.storage_engine, "" )
                .setConfig( GraphDatabaseSettings.pagecache_memory, pcMemory )
                .build();
        DatabaseManagementService toDbms = new EnterpriseDatabaseManagementServiceBuilder( toHome.toFile() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false )
                .setConfig( GraphDatabaseSettings.pagecache_memory, pcMemory )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, "false" )
                .setConfig( GraphDatabaseSettings.check_point_policy, CONTINUOUS )
                .setConfig( GraphDatabaseSettings.check_point_iops_limit, -1 )
                .setConfig( GraphDatabaseSettings.storage_engine, "Freki" )
                .build();
        try
        {
<<<<<<< HEAD
            copyDatabase( fromDbms.database( DEFAULT_DATABASE_NAME ), toDbms.database( DEFAULT_DATABASE_NAME ) );
=======
            GraphDatabaseService fromDb = fromDbms.database( DEFAULT_DATABASE_NAME );
            GraphDatabaseAPI toDb = (GraphDatabaseAPI) toDbms.database( DEFAULT_DATABASE_NAME );
            copyDatabase( fromDb, toDb, validate );
            if ( statistics )
            {
                System.out.println( "Printing statistics for migrated store." );
                toDb.getDependencyResolver().resolveDependency( FrekiStorageEngine.class ).analysis().dumpStoreStats();
            }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        }
        finally
        {
            fromDbms.shutdown();
            toDbms.shutdown();
        }
    }

<<<<<<< HEAD
    private static void copyDatabase( GraphDatabaseService from, GraphDatabaseService to )
    {
        try ( Transaction fromTx = from.beginTx() )
        {
            copyData( to, fromTx );
=======
    private static void copyDatabase( GraphDatabaseService from, GraphDatabaseService to, boolean validate )
    {
        try ( Transaction fromTx = from.beginTx() )
        {
            copyData( to, fromTx, validate );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            copySchema( to, fromTx );
            fromTx.commit();
        }
        System.out.println( "Database copied" );
    }

<<<<<<< HEAD
    private static void copyData( GraphDatabaseService to, Transaction fromTx )
=======
    private static void copyData( GraphDatabaseService to, Transaction fromTx, boolean validate )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    {
        InternalTransaction tx = (InternalTransaction) fromTx;
        long numNodes = tx.kernelTransaction().dataRead().nodesGetCount();
        long nodesPerMap = 10_000_000;
        int numMaps = toIntExact( numNodes / nodesPerMap ) + 1;
        MutableLongLongMap[] fromToNodeIdTable = new MutableLongLongMap[numMaps];
        for ( int i = 0; i < fromToNodeIdTable.length; i++ )
        {
            fromToNodeIdTable[i] = LongLongMaps.mutable.empty();
        }

        try ( ResourceIterator<Node> nodes = fromTx.getAllNodes().iterator() )
        {
            for ( int batch = 0; nodes.hasNext(); batch++ )
            {
                try ( Transaction toTx = to.beginTx() )
                {
                    for ( int i = 0; i < 100_000 && nodes.hasNext(); i++ )
                    {
                        Node fromNode = nodes.next();
                        Node toNode = copyNodeData( fromNode, toTx );
<<<<<<< HEAD
                        fromToNodeIdTable[ toIntExact( fromNode.getId() % numMaps ) ].put( fromNode.getId(), toNode.getId() );
=======
                        if ( fromNode.getId() != toNode.getId() )
                        {
                            fromToNodeIdTable[toIntExact( fromNode.getId() % numMaps )].put( fromNode.getId(), toNode.getId() );
                        }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                    }
                    toTx.commit();
                    System.out.println( "node batch " + batch + " completed" );
                }
            }
        }
<<<<<<< HEAD
=======
        if ( validate )
        {
            System.out.println( "Validating Nodes ");
            try ( Transaction toTx = to.beginTx() )
            {
                for ( Node fromNode : fromTx.getAllNodes() )
                {
                    long toNodeId = fromToNodeIdTable[toIntExact( fromNode.getId() % numMaps )].getIfAbsent( fromNode.getId(), fromNode.getId() );
                    Node toNode = toTx.getNodeById( toNodeId );
                    compareNodes( fromNode, toNode, true, true, false );
                }
            }
            System.out.println( "Store looks fine");
        }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        try ( ResourceIterator<Relationship> relationships = fromTx.getAllRelationships().iterator() )
        {
            for ( int batch = 0; relationships.hasNext(); batch++ )
            {
                try ( Transaction toTx = to.beginTx() )
                {
                    for ( int i = 0; i < 100_000 && relationships.hasNext(); i++ )
                    {
                        Relationship fromRelationship = relationships.next();
<<<<<<< HEAD
                        long startId = fromToNodeIdTable[toIntExact( fromRelationship.getStartNodeId() % numMaps )].get( fromRelationship.getStartNodeId() );
                        long endId = fromToNodeIdTable[toIntExact( fromRelationship.getEndNodeId() % numMaps )].get( fromRelationship.getEndNodeId() );

                        Node toStartNode = toTx.getNodeById( startId );
                        Node toEndNode = toTx.getNodeById( endId );
                        Relationship toRelationship = toStartNode.createRelationshipTo( toEndNode, fromRelationship.getType() );
                        fromRelationship.getAllProperties().forEach( toRelationship::setProperty );
=======
                        long startId = fromToNodeIdTable[toIntExact( fromRelationship.getStartNodeId() % numMaps )]
                                .getIfAbsent( fromRelationship.getStartNodeId(), fromRelationship.getStartNodeId() );
                        long endId = fromToNodeIdTable[toIntExact( fromRelationship.getEndNodeId() % numMaps )]
                                .getIfAbsent( fromRelationship.getEndNodeId(), fromRelationship.getEndNodeId() );

                        Node toStartNode = toTx.getNodeById( startId );
                        Node toEndNode = toTx.getNodeById( endId );
                        copyProperties( fromRelationship, toStartNode.createRelationshipTo( toEndNode, fromRelationship.getType() ) );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                    }
                    toTx.commit();
                    System.out.println( "relationship batch " + batch + " completed" );
                }
            }
        }
<<<<<<< HEAD
=======

        if ( validate )
        {
            System.out.println( "Validating store");
            try ( Transaction toTx = to.beginTx() )
            {
                for ( Node fromNode : fromTx.getAllNodes() )
                {
                    long toNodeId =  fromToNodeIdTable[toIntExact( fromNode.getId() % numMaps )].getIfAbsent( fromNode.getId(), fromNode.getId() );
                    Node toNode = toTx.getNodeById( toNodeId );
                    compareNodes( fromNode, toNode, true, true, true );
                }
            }
            System.out.println( "Store looks fine");
        }
    }

    private static void compareNodes( Node fromNode, Node toNode, boolean checkLabels, boolean checkProps, boolean checkDegrees )
    {
        try
        {
            if ( checkLabels )
            {
                HashSet<String> fromLabels = new HashSet<>();
                HashSet<String> toLabels = new HashSet<>();
                fromNode.getLabels().forEach( l -> fromLabels.add( l.name() ) );
                toNode.getLabels().forEach( l -> toLabels.add( l.name() ) );
                if ( !fromLabels.equals( toLabels ) )
                {
                    throw new RuntimeException(
                            "Broken labels " + toNode + toLabels + " should be " + fromNode + fromLabels + " diff " + setDiff( fromLabels, toLabels ) );
                }
            }

            if ( checkProps )
            {
                HashMap<String,Value> fromProps = new HashMap<>();
                HashMap<String,Value> toProps = new HashMap<>();
                fromNode.getAllProperties().forEach( ( s, o ) -> fromProps.put( s, Values.of( o ) ) );
                toNode.getAllProperties().forEach( ( s, o ) -> toProps.put( s, Values.of( o ) ) );
                if ( !fromProps.equals( toProps ) )
                {
                    throw new RuntimeException(
                            "Broken properties " + toNode + toProps + " should be " + fromNode + fromProps + " diff " + mapDiff( fromProps, toProps ) );
                }
            }

            if ( checkDegrees )
            {
                if ( fromNode.getDegree() != toNode.getDegree() )
                {
                    throw new RuntimeException(
                            "Broken relationships (degrees) " + toNode + ", " + toNode.getDegree() + " should be " + fromNode + ", " + fromNode.getDegree() +
                                    " diff " + degreesDiff( fromNode, toNode ) );
                }
            }
        }
        catch ( RuntimeException e )
        {
            System.err.printf( "Validation failed for %s --> %s listing contents, from:%n%s%n%nto:%n%s%n", fromNode, toNode,
                    contentsOfNode( fromNode ), contentsOfNode( toNode ) );
            throw e;
        }
    }

    private static String contentsOfNode( Node node )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "Labels:" );
        node.getLabels().forEach( label -> builder.append( format( "%n  " ) ).append( label.name() ) );
        builder.append( format( "%nProperties:" ) );
        node.getAllProperties().forEach( ( key, value ) -> builder.append( format( "%n  %s=%s", key, value ) ) );
        builder.append( format( "%nRelationships:" ) );
        TreeSet<RelationshipType> types = new TreeSet<>( comparing( RelationshipType::name ) );
        node.getRelationshipTypes().forEach( types::add );
        for ( RelationshipType type : types )
        {
            node.getRelationships( Direction.OUTGOING, type ).forEach( rel -> builder.append( format( "%n  %s", rel ) ) );
        }
        return builder.append( format( "%n" ) ).toString();
    }

    private static String degreesDiff( Node fromNode, Node toNode )
    {
        Set<String> fromTypes = new HashSet<>();
        Set<String> toTypes = new HashSet<>();
        fromNode.getRelationshipTypes().forEach( type -> fromTypes.add( type.name() ) );
        toNode.getRelationshipTypes().forEach( type -> toTypes.add( type.name() ) );
        if ( !fromTypes.equals( toTypes ) )
        {
            return "Relationship types differ: " + setDiff( fromTypes, toTypes );
        }

        StringBuilder builder = new StringBuilder();
        for ( String typeName : fromTypes )
        {
            RelationshipType type = RelationshipType.withName( typeName );
            checkDegreeDiff( fromNode, toNode, builder, type, Direction.OUTGOING );
            checkDegreeDiff( fromNode, toNode, builder, type, Direction.INCOMING );
            checkDegreeDiff( fromNode, toNode, builder, type, Direction.BOTH );
        }
        return builder.toString();
    }

    private static void checkDegreeDiff( Node fromNode, Node toNode, StringBuilder builder, RelationshipType type, Direction direction )
    {
        int from = fromNode.getDegree( type, direction );
        int to = toNode.getDegree( type, direction );
        if ( from != to )
        {
            builder.append( format( "degree:%s,%s:%d vs %d", type.name(), direction.name(), from, to ) );
        }
    }

    private static <T> String setDiff( Set<T> from, Set<T> to )
    {
        StringBuilder builder = new StringBuilder();
        Set<T> combined = new HashSet<>( from );
        combined.removeAll( to );
        combined.forEach( label -> builder.append( format( "%n<%s", label ) ) );
        combined = new HashSet<>( to );
        combined.removeAll( from );
        combined.forEach( label -> builder.append( format( "%n>%s", label ) ) );
        return builder.toString();
    }

    private static <T> String mapDiff( Map<String,T> from, Map<String,T> to )
    {
        StringBuilder builder = new StringBuilder();
        Set<String> allKeys = new HashSet<>( from.keySet() );
        allKeys.addAll( to.keySet() );
        for ( String key : allKeys )
        {
            T fromValue = from.get( key );
            T toValue = to.get( key );
            if ( toValue == null )
            {
                builder.append( format( "%n<%s=%s", key, fromValue ) );
            }
            else if ( fromValue == null )
            {
                builder.append( format( "%n>%s=%s", key, toValue ) );
            }
            else if ( !fromValue.equals( toValue ) )
            {
                builder.append( format( "%n!%s=%s vs %s", key, fromValue, toValue ) );
            }
        }
        return builder.toString();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    private static void copySchema( GraphDatabaseService to, Transaction fromTx )
    {
        System.out.println( "Copying indexes..." );
        try ( Transaction toTx = to.beginTx() )
        {
            for ( IndexDefinition fromIndex : fromTx.schema().getIndexes() )
            {
                System.out.println( "  Copying index " + fromIndex );
                if ( !fromIndex.isConstraintIndex() )
                {
                    copyIndexDefinition( fromIndex, toTx );
                }
            }
            toTx.commit();
        }
        System.out.println( "Copying constraints..." );
        for ( ConstraintDefinition fromConstraint : fromTx.schema().getConstraints() )
        {
            try ( Transaction toTx = to.beginTx() )
            {
                System.out.println( "  Copying constraint " + fromConstraint );
                copyConstraintDefinition( fromConstraint, toTx );
                toTx.commit();
            }
        }
        System.out.println( "Awaiting indexes to build..." );
        try ( Transaction toTx = to.beginTx() )
        {
            toTx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            toTx.commit();
        }
    }

    private static void copyIndexDefinition( IndexDefinition fromIndex, Transaction toTx )
    {
        IndexCreator indexCreator;
        if ( fromIndex.isRelationshipIndex() )
        {
            indexCreator = toTx.schema().indexFor( relationshipTypesAsArray( fromIndex.getRelationshipTypes() ) );
        }
        else
        {
            indexCreator = toTx.schema().indexFor( labelsAsArray( fromIndex.getLabels() ) );
        }
        indexCreator = indexCreator
                .withName( fromIndex.getName() )
                .withIndexType( fromIndex.getIndexType() )
                .withIndexConfiguration( fromIndex.getIndexConfiguration() );
        for ( String propertyKey : fromIndex.getPropertyKeys() )
        {
            indexCreator = indexCreator.on( propertyKey );
        }
        indexCreator.create();
    }

    private static void copyConstraintDefinition( ConstraintDefinition fromConstraint, Transaction toTx )
    {
        ConstraintCreator constraintCreator;
        switch ( fromConstraint.getConstraintType() )
        {
        case NODE_KEY:
        {
            constraintCreator = toTx.schema().constraintFor( fromConstraint.getLabel() ).withName( fromConstraint.getName() );
            for ( String propertyKey : fromConstraint.getPropertyKeys() )
            {
                constraintCreator = constraintCreator.assertPropertyIsNodeKey( propertyKey );
            }
            break;
        }
        case UNIQUENESS:
        {
            constraintCreator = toTx.schema().constraintFor( fromConstraint.getLabel() ).withName( fromConstraint.getName() );
            for ( String propertyKey : fromConstraint.getPropertyKeys() )
            {
                constraintCreator = constraintCreator.assertPropertyIsUnique( propertyKey );
            }
            break;
        }
        case NODE_PROPERTY_EXISTENCE:
        {
            constraintCreator = toTx.schema().constraintFor( fromConstraint.getLabel() ).withName( fromConstraint.getName() );
            for ( String propertyKey : fromConstraint.getPropertyKeys() )
            {
                constraintCreator = constraintCreator.assertPropertyExists( propertyKey );
            }
            break;
        }
        case RELATIONSHIP_PROPERTY_EXISTENCE:
        {
            constraintCreator = toTx.schema().constraintFor( fromConstraint.getRelationshipType() ).withName( fromConstraint.getName() );
            for ( String propertyKey : fromConstraint.getPropertyKeys() )
            {
                constraintCreator = constraintCreator.assertPropertyIsUnique( propertyKey );
            }
            break;
        }
        default:
            throw new RuntimeException( "Unrecognized constraint type: " + fromConstraint.getConstraintType() );
        }
        constraintCreator.create();
    }

    private static Node copyNodeData( Node fromNode, Transaction toTx )
    {
<<<<<<< HEAD
        Node node = toTx.createNode( labelsAsArray( fromNode.getLabels() ) );
        fromNode.getAllProperties().forEach( node::setProperty );
        return node;
=======
        return copyProperties( fromNode, toTx.createNode( labelsAsArray( fromNode.getLabels() ) ) );
    }

    private static <E extends Entity> E copyProperties( E from, E to )
    {
        from.getAllProperties().forEach( to::setProperty );
        return to;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    private static Label[] labelsAsArray( Iterable<Label> labels )
    {
        return asList( labels ).toArray( new Label[0] );
    }

    private static RelationshipType[] relationshipTypesAsArray( Iterable<RelationshipType> types )
    {
        return asList( types ).toArray( new RelationshipType[0] );
    }
}
