/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bootstrap;

import com.neo4j.fabric.localdb.FabricDatabaseManager;

import org.neo4j.collection.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class FabricServicesBootstrap
{

    public FabricServicesBootstrap( LifeSupport lifeSupport, Dependencies dependencies )
    {
        dependencies.satisfyDependency( new FabricDatabaseManager() );
    }
}
