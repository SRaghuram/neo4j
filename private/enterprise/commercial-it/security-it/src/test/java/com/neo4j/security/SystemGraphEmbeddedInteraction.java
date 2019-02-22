/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.server.security.enterprise.auth.EmbeddedInteraction;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;

import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.rule.TestDirectory;

class SystemGraphEmbeddedInteraction extends EmbeddedInteraction
{
    SystemGraphEmbeddedInteraction( Map<String, String> config, TestDirectory testDirectory ) throws Throwable
    {
        TestCommercialGraphDatabaseFactory factory = new TestCommercialGraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDirectory.databaseDir() );
        init( builder, config );
    }

    @Override
    protected void init( GraphDatabaseBuilder builder, Map<String, String> config ) throws Throwable
    {
        builder.setConfig( SecuritySettings.auth_provider, SecuritySettings.SYSTEM_GRAPH_REALM_NAME );
        super.init( builder, config );
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return db.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
    }
}
