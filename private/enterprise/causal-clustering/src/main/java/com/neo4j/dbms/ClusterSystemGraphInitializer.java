/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;

public class ClusterSystemGraphInitializer extends EnterpriseSystemGraphInitializer
{
    public ClusterSystemGraphInitializer( DatabaseManager<?> databaseManager, Config config )
    {
        super( databaseManager, config );
    }

    @Override
    public void initializeSystemGraph( GraphDatabaseService system ) throws Exception
    {
        super.initializeSystemGraph( system );
        clearClusterProperties( system );
    }

    private void clearClusterProperties( GraphDatabaseService system )
    {
        try ( Transaction tx = system.beginTx() )
        {
            try ( ResourceIterator<Node> databaseItr = tx.findNodes( DATABASE_LABEL ) )
            {
                while ( databaseItr.hasNext() )
                {
                    Node database = databaseItr.next();
                    database.removeProperty( ClusterSystemGraphDbmsModel.INITIAL_MEMBERS );
                    database.removeProperty( ClusterSystemGraphDbmsModel.STORE_CREATION_TIME );
                    database.removeProperty( ClusterSystemGraphDbmsModel.STORE_RANDOM_ID );
                    database.removeProperty( ClusterSystemGraphDbmsModel.STORE_VERSION );
                }
            }
            tx.commit();
        }
    }
}
