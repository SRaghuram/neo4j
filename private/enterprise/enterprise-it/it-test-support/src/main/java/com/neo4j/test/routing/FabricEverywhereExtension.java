/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.routing;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.util.FeatureToggles;

import static org.neo4j.fabric.FabricDatabaseManager.FABRIC_BY_DEFAULT_DEFAULT_VALUE;
import static org.neo4j.fabric.FabricDatabaseManager.FABRIC_BY_DEFAULT_FLAG_NAME;

public class FabricEverywhereExtension implements BeforeAllCallback, AfterAllCallback
{
    @Override
    public void beforeAll( ExtensionContext context )
    {
        FeatureToggles.set( FabricDatabaseManager.class, FABRIC_BY_DEFAULT_FLAG_NAME, true );
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        FeatureToggles.set( FabricDatabaseManager.class, FABRIC_BY_DEFAULT_FLAG_NAME, FABRIC_BY_DEFAULT_DEFAULT_VALUE );
    }
}
