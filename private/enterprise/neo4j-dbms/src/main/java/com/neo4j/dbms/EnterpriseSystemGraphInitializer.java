/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;
import org.neo4j.graphdb.GraphDatabaseService;

public class EnterpriseSystemGraphInitializer extends DefaultSystemGraphInitializer
{
    public EnterpriseSystemGraphInitializer( DatabaseManager<?> databaseManager, Config config )
    {
        super( databaseManager, config, false );
    }

    @Override
    public void initializeSystemGraph() throws Exception
    {
        super.initializeSystemGraph();
    }

    @Override
    public void initializeSystemGraph( GraphDatabaseService system ) throws Exception
    {
        super.initializeSystemGraph( system );
    }
}
