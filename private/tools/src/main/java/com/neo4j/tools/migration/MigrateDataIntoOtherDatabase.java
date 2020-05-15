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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
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
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.CheckpointPolicy.CONTINUOUS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.asList;

public class MigrateDataIntoOtherDatabase
{
    public static void main( String[] args )
    {
        Path fromHome = Path.of( args[0] );
        Path toHome = Path.of( args[1] );
        migrate( fromHome, toHome, true );
    }

    public static void migrate( Path fromHome, Path toHome, boolean validate )
    {
        assert !(fromHome.equals( toHome ));
        System.out.println( format( "Migrating %s to Freki store: %s", fromHome, toHome ) );
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
            copyDatabase( fromDbms.database( DEFAULT_DATABASE_NAME ), toDbms.database( DEFAULT_DATABASE_NAME ), validate );
        }
        finally
        {
            fromDbms.shutdown();
            toDbms.shutdown();
        }
    }

    private static void copyDatabase( GraphDatabaseService from, GraphDatabaseService to, boolean validate )
    {
        try ( Transaction fromTx = from.beginTx() )
        {
            copyData( to, fromTx, validate );
            copySchema( to, fromTx );
            fromTx.commit();
        }
        System.out.println( "Database copied" );
    }

    private static void copyData( GraphDatabaseService to, Transaction fromTx, boolean validate )
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
                        fromToNodeIdTable[toIntExact( fromNode.getId() % numMaps )].put( fromNode.getId(), toNode.getId() );
                    }
                    toTx.commit();
                    System.out.println( "node batch " + batch + " completed" );
                }
            }
        }
        if ( validate )
        {
            System.out.println( "Validating Nodes ");
            try ( Transaction toTx = to.beginTx() )
            {
                for ( Node fromNode : fromTx.getAllNodes() )
                {
                    long toNodeId =  fromToNodeIdTable[toIntExact( fromNode.getId() % numMaps )].get( fromNode.getId() );
                    Node toNode = toTx.getNodeById( toNodeId );
                    compareNodes( fromNode, toNode, true, true, false );
                }
            }
            System.out.println( "Store looks fine");
        }
        try ( ResourceIterator<Relationship> relationships = fromTx.getAllRelationships().iterator() )
        {
            for ( int batch = 0; relationships.hasNext(); batch++ )
            {
                try ( Transaction toTx = to.beginTx() )
                {
                    for ( int i = 0; i < 100_000 && relationships.hasNext(); i++ )
                    {
                        Relationship fromRelationship = relationships.next();
                        long startId = fromToNodeIdTable[toIntExact( fromRelationship.getStartNodeId() % numMaps )].get( fromRelationship.getStartNodeId() );
                        long endId = fromToNodeIdTable[toIntExact( fromRelationship.getEndNodeId() % numMaps )].get( fromRelationship.getEndNodeId() );

                        Node toStartNode = toTx.getNodeById( startId );
                        Node toEndNode = toTx.getNodeById( endId );
                        Relationship toRelationship = toStartNode.createRelationshipTo( toEndNode, fromRelationship.getType() );
                        fromRelationship.getAllProperties().forEach( toRelationship::setProperty );
                    }
                    toTx.commit();
                    System.out.println( "relationship batch " + batch + " completed" );
                }
            }
        }

        if ( validate )
        {
            System.out.println( "Validating store");
            try ( Transaction toTx = to.beginTx() )
            {
                for ( Node fromNode : fromTx.getAllNodes() )
                {
                    long toNodeId =  fromToNodeIdTable[toIntExact( fromNode.getId() % numMaps )].get( fromNode.getId() );
                    Node toNode = toTx.getNodeById( toNodeId );
                    compareNodes( fromNode, toNode, true, true, true );
                }
            }
            System.out.println( "Store looks fine");
        }
    }

    private static void compareNodes( Node fromNode, Node toNode, boolean checkLabels, boolean checkProps, boolean checkDegrees )
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
                throw new RuntimeException( "Broken properties " + toNode + toProps + " should be " + fromNode + fromProps +
                        " diff " + mapDiff( fromProps, toProps ) );
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
        Node node = toTx.createNode( labelsAsArray( fromNode.getLabels() ) );
        fromNode.getAllProperties().forEach( node::setProperty );
        return node;
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
