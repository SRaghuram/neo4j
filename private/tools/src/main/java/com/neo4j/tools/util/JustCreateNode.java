package com.neo4j.tools.util;

import java.io.File;
import java.io.IOException;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class JustCreateNode
{
    public static void main( String[] args ) throws IOException
    {
        File homeDirectory = new File( "C:/Users/Mattias/Desktop/home" );
        FileUtils.deleteRecursively( homeDirectory );
        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder( homeDirectory ).build();
        Label label = Label.label( "Hello token world" );
        Label label2 = Label.label( "Indeed coolness" );
        try
        {
            GraphDatabaseService db = dbms.database( DEFAULT_DATABASE_NAME );
            long[] nodeIds = new long[5];
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodeIds.length; i++ )
                {
                    nodeIds[i] = tx.createNode( label, label2 ).getId();
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
                        System.out.println( "  Has label '" + nodeLabel.name() + "'" );
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
}
