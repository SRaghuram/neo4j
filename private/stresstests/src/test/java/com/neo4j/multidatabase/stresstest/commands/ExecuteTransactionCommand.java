/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class ExecuteTransactionCommand extends DatabaseManagerCommand
{
    public ExecuteTransactionCommand( DatabaseManagementService dbms, String databaseName )
    {
        super( dbms, databaseName );
    }

    @Override
    void execute( DatabaseManagementService dbms, String databaseName )
    {
        GraphDatabaseFacade databaseFacade;

        try
        {
            databaseFacade = (GraphDatabaseFacade) dbms.database( databaseName );
        }
        catch ( DatabaseNotFoundException e )
        {
            return; //A non existent database is ignored
        }

        try ( Transaction transaction = databaseFacade.beginTx() )
        {
            Node node1 = transaction.createNode();
            Node node2 = transaction.createNode();
            node1.setProperty( "a", "b" );
            node2.setProperty( "c", "d" );
            node1.createRelationshipTo( node2, RelationshipType.withName( "some" ) );
            transaction.commit();
        }
        catch ( IllegalStateException e )
        {
            if ( e.getMessage() == null )
            {
                throw e;
            }
            if ( !e.getMessage().contains( "Kernel is not running" ) )
            {
                throw e;
            }
        }
    }
}
