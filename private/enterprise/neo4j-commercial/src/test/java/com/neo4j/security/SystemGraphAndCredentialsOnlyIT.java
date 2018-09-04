/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;

import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.auth.integration.bolt.NativeAndCredentialsOnlyIT;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.TestGraphDatabaseFactory;

public class SystemGraphAndCredentialsOnlyIT extends NativeAndCredentialsOnlyIT
{
    @Override
    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return super.getSettingsFunction()
                .andThen( settings -> settings.put( SecuritySettings.auth_providers, "system-graph,plugin-TestCredentialsOnlyPlugin" ) );
    }

    @Override
    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestCommercialGraphDatabaseFactory();
    }
}
