/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.multidatabase.stresstest.commands;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class ExecuteTransactionCommand extends DatabaseManagerCommand
{
    public ExecuteTransactionCommand( DatabaseManager<?> manager, DatabaseId databaseId )
    {
        super( manager, databaseId );
    }

    @Override
    void execute( DatabaseManager<?> manager, DatabaseId databaseId )
    {
        var databaseContext = manager.getDatabaseContext( databaseId );
        if ( databaseContext.isPresent() )
        {
            GraphDatabaseFacade databaseFacade = databaseContext.get().databaseFacade();
            try ( Transaction transaction = databaseFacade.beginTx() )
            {
                Node node1 = databaseFacade.createNode();
                Node node2 = databaseFacade.createNode();
                node1.setProperty( "a", "b" );
                node2.setProperty( "c", "d" );
                node1.createRelationshipTo( node2, RelationshipType.withName( "some" ) );
                transaction.success();
            }
        }
    }
}
