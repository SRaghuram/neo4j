/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.migration;

import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.impl.factory.primitive.IntIntMaps;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
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

import static java.lang.Math.toIntExact;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.asList;

public class MigrateDataIntoOtherDatabase
{
    private static final String DB_NAME = "Freki.newdb";

    public static void main( String[] args )
    {
        File homeDirectory = new File( args[0] );
        DatabaseManagementService dbms = new EnterpriseDatabaseManagementServiceBuilder( homeDirectory ).build();
        try
        {
            try
            {
                dbms.dropDatabase( DB_NAME );
            }
            catch ( DatabaseNotFoundException e )
            {
                // This is OK
            }
            dbms.createDatabase( DB_NAME );
            copyDatabase( dbms.database( DEFAULT_DATABASE_NAME ), dbms.database( DB_NAME ) );
        }
        finally
        {
            dbms.shutdown();
        }
    }

    private static void copyDatabase( GraphDatabaseService from, GraphDatabaseService to )
    {
        try ( Transaction fromTx = from.beginTx() )
        {
            copyData( to, fromTx );
            copySchema( to, fromTx );
            fromTx.commit();
        }
        System.out.println( "Database copied" );
    }

    private static void copyData( GraphDatabaseService to, Transaction fromTx )
    {
        MutableIntIntMap fromToNodeIdTable = IntIntMaps.mutable.empty();
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
                        fromToNodeIdTable.put( toIntExact( fromNode.getId() ), toIntExact( toNode.getId() ) );
                    }
                    toTx.commit();
                    System.out.println( "node batch " + batch + " completed" );
                }
            }
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
                        Node toStartNode = toTx.getNodeById( fromToNodeIdTable.get( toIntExact( fromRelationship.getStartNodeId() ) ) );
                        Node toEndNode = toTx.getNodeById( fromToNodeIdTable.get( toIntExact( fromRelationship.getEndNodeId() ) ) );
                        Relationship toRelationship = toStartNode.createRelationshipTo( toEndNode, fromRelationship.getType() );
                        fromRelationship.getAllProperties().forEach( toRelationship::setProperty );
                    }
                    toTx.commit();
                    System.out.println( "relationship batch " + batch + " completed" );
                }
            }
        }
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
