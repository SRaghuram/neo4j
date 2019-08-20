/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.localdb;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class FabricDatabaseManager extends LifecycleAdapter
{

    public boolean isFabricDatabase( String databaseName )
    {
        return false;
    }

    public void ensureFabricDatabasesExist( GraphDatabaseService system, boolean update )
    {

    }
}
