/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security.configuration;

import java.util.List;
import java.util.function.Function;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.SYSTEM_GRAPH_REALM_NAME;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.auth_providers;

@Description( "Commercial edition security settings" )
public class CommercialSecuritySettings implements LoadableConfig
{
    @Description( "Enable authentication via system graph authentication provider." )
    @Internal
    public static final Setting<Boolean> system_graph_authentication_enabled =
            derivedSetting( "dbms.security.system_graph.authentication_enabled", auth_providers, containsSystemGraphRealm(), BOOLEAN );

    @Description( "Enable authorization via system graph authorization provider." )
    @Internal
    public static final Setting<Boolean> system_graph_authorization_enabled =
            derivedSetting( "dbms.security.system_graph.authorization_enabled", auth_providers, containsSystemGraphRealm(), BOOLEAN );

    public static boolean isSystemDatabaseEnabled( Config config )
    {
        return containsSystemGraphRealm().apply( config.get( auth_providers ) );
    }

    private static Function<List<String>,Boolean> containsSystemGraphRealm()
    {
        return providers -> providers.contains( SYSTEM_GRAPH_REALM_NAME );
    }
}
