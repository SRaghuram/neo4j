/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DefaultSystemGraphComponent;
import org.neo4j.graphdb.Node;

/***
 * This is an enterprise component for databases.
 * It is building on {@link DefaultSystemGraphComponent}, with the only difference being that
 * the old default database is not stopped when a new default database is chosen.
 */
public class EnterpriseSystemGraphComponent extends DefaultSystemGraphComponent
{
    public EnterpriseSystemGraphComponent( Config config )
    {
        super( config );
    }

    /**
     * Enterprise edition (and other editions) do not stop the database
     */
    protected void maybeStopDatabase( Node oldDatabaseNode )
    {
    }
}
