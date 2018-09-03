/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security.configuration;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.SYSTEM_GRAPH_REALM_NAME;

@Description( "Commercial edition security settings" )
public class CommercialSecuritySettings implements LoadableConfig
{
    @Description( "Enable authentication via system-graph provider." )
    @Internal
    public static final Setting<Boolean> system_graph_authentication_enabled =
            derivedSetting( "dbms.security.system_graph.authentication_enabled", SecuritySettings.auth_providers,
                    providers -> providers.contains(SYSTEM_GRAPH_REALM_NAME ) , BOOLEAN );

    @Description( "Enable authorization via system-graph authorization provider." )
    @Internal
    public static final Setting<Boolean> system_graph_authorization_enabled =
            derivedSetting( "dbms.security.system_graph.authorization_enabled", SecuritySettings.auth_providers,
                    providers -> providers.contains( SYSTEM_GRAPH_REALM_NAME ), BOOLEAN );

    public static boolean isSystemDatabaseEnabled( Config config )
    {
        return config.get( system_graph_authentication_enabled ) || config.get( system_graph_authorization_enabled );
    }
}
