package com.neo4j.security.configuration;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.derivedSetting;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.SYSTEM_GRAPH_REALM_NAME;

public class CommercialSecuritySettings
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
}
