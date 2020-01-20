/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.util;

import java.io.File;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class JustCreateNode
{
    public static void main( String[] args ) throws Exception
    {
        File homeDirectory = new File( "C:/Users/Matilas/Desktop/home" );
        FileUtils.deleteRecursively( homeDirectory );
        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder( homeDirectory ).build();
        Label label = Label.label( "Hello token world" );
        Label label2 = Label.label( "Indeed coolness" );
        Map<String, Object> nodeProperties = Map.of(
//                "name", "Valdemar",
//                "scalar_huge", 1L << 48,
//                 "scalar_medium", 1 << 24,
//                 "scalar_small", 1 << 12,
                 "scalar_minimal", 1L << 4
//                 "number", 9.9
        );
        Map<String, Object> relationshipProperties = Map.of(
//                "name", "Mattias",
//                "scalar_huge", 1L << 47,
//                 "scalar_medium", 1 << 23,
//                 "scalar_small", 1 << 11,
                 "scalar_minimal", 1L << 3
//                 "number", 8.8
        );
        try
        {
            GraphDatabaseService db = dbms.database( DEFAULT_DATABASE_NAME );
            long[] nodeIds = new long[2];
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodeIds.length; i++ )
                {
                    Node node = tx.createNode( label, label2 );
                    nodeProperties.forEach( node::setProperty );
                    nodeIds[i] = node.getId();

                    Relationship relationship =
                            node.createRelationshipTo( tx.getNodeById( nodeIds[Math.max( i - 1, 0 )] ), RelationshipType.withName( "likes" ) );
                    relationshipProperties.forEach( relationship::setProperty );
                    node.createRelationshipTo( tx.getNodeById( nodeIds[Math.max( i - 1, 0 )] ), RelationshipType.withName( "knows" ) );
                }
                tx.commit();
            }
            try ( Transaction tx = db.beginTx() )
            {
                for ( long nodeId : nodeIds )
                {
                    Node node = tx.getNodeById( nodeId );
                    System.out.println( node );
                    for ( Label nodeLabel : node.getLabels() )
                    {
                        System.out.println( "  :'" + nodeLabel.name() + "'" );
                    }
                    printProperties( "  ", node.getAllProperties() );

                    for ( Relationship relationship : node.getRelationships() )
                    {
                        System.out.println( "  " + relationship );
                        printProperties( "    ", relationship.getAllProperties() );
                    }
                }
            }
        }
        catch ( Throwable t )
        {
            t.printStackTrace();
            throw t;
        }
        finally
        {
            dbms.shutdown();
        }
    }

    private static void printProperties( String prefix, Map<String,Object> readProperties )
    {
        readProperties.forEach( ( key, value ) -> System.out.println( prefix + key + "=" + value ) );
    }
}
