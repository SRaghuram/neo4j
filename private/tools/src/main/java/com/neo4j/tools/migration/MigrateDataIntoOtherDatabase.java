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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

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
            MutableIntIntMap fromToNodeIdTable = IntIntMaps.mutable.empty();
            try ( ResourceIterator<Node> nodes = fromTx.getAllNodes().iterator() )
            {
                while ( nodes.hasNext() )
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
                        System.out.println( "node batch completed" );
                    }
                }
            }
            try ( ResourceIterator<Relationship> relationships = fromTx.getAllRelationships().iterator() )
            {
                while ( relationships.hasNext() )
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
                        System.out.println( "relationship batch completed" );
                    }
                }
            }

            // TODO schema

            fromTx.commit();
        }
    }

    private static Node copyNodeData( Node fromNode, Transaction toTx )
    {
        Node node = toTx.createNode( asList( fromNode.getLabels() ).toArray( new Label[0] ) );
        fromNode.getAllProperties().forEach( node::setProperty );
        return node;
    }
}
