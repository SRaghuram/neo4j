/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security.configuration;

import java.util.List;
import java.util.function.Function;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.SYSTEM_GRAPH_REALM_NAME;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.auth_providers;

@Description( "Commercial edition security settings" )
public class CommercialSecuritySettings implements LoadableConfig
{
    public static boolean isSystemDatabaseEnabled( Config config )
    {
        return containsSystemGraphRealm().apply( config.get( auth_providers ) );
    }

    private static Function<List<String>,Boolean> containsSystemGraphRealm()
    {
        return providers -> providers.contains( SYSTEM_GRAPH_REALM_NAME );
    }
}
