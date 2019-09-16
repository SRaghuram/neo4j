/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;

public class EnterpriseSystemGraphInitializer extends DefaultSystemGraphInitializer
{
    public EnterpriseSystemGraphInitializer( DatabaseManager<?> databaseManager, Config config )
    {
        super( databaseManager, config, false );
    }
}
