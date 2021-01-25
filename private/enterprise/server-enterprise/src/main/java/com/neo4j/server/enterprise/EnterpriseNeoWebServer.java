/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.server.enterprise.modules.EnterpriseAuthorizationModule;
import com.neo4j.server.rest.ClusterModule;
import com.neo4j.server.rest.LegacyManagementModule;
import com.neo4j.server.rest.causalclustering.ClusteringDatabaseService;
import com.neo4j.server.rest.causalclustering.ClusteringDbmsService;
import com.neo4j.server.rest.causalclustering.LegacyClusteringRedirectService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.CommunityNeoWebServer;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static com.neo4j.server.rest.EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs;

public class EnterpriseNeoWebServer extends CommunityNeoWebServer
{
    public EnterpriseNeoWebServer( DatabaseManagementService managementService, Dependencies globalDependencies, Config config,
            LogProvider userLogProvider, DbmsInfo dbmsInfo )
    {
        super( managementService, globalDependencies, config, userLogProvider, dbmsInfo );
    }

    @Override
    protected AuthorizationModule createAuthorizationModule()
    {
        return new EnterpriseAuthorizationModule( webServer, authManagerSupplier, userLogProvider, getConfig(),
                getUriWhitelist() );
    }

    @Override
    protected DBMSModule createDBMSModule()
    {
        // Bolt port isn't available until runtime, so defer loading until then
        Supplier<DiscoverableURIs> discoverableURIs =
                () -> enterpriseDiscoverableURIs( getConfig(), connectorPortRegister );
        return new DBMSModule( webServer, getConfig(), discoverableURIs, userLogProvider );
    }

    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        var config = getConfig();
        var enabledModules = config.get( ServerSettings.http_enabled_modules );
        var serverModules = new ArrayList<ServerModule>();

        if ( enabledModules.contains( ConfigurableServerModules.ENTERPRISE_MANAGEMENT_ENDPOINTS ) )
        {
            serverModules.add( new ClusterModule( webServer, getConfig() ) );
            serverModules.add( new LegacyManagementModule( webServer, getConfig() ) );
        }

        super.createServerModules().forEach( serverModules::add );
        return serverModules;
    }

    @Override
    protected List<Pattern> getUriWhitelist()
    {
        var result = new ArrayList<>( super.getUriWhitelist() );
        if ( !getConfig().get( CausalClusteringSettings.status_auth_enabled ) )
        {
            result.add( ClusteringDbmsService.dbmsClusterUriPattern() );
            result.add( ClusteringDatabaseService.databaseClusterUriPattern( getConfig() ) );
            result.add( LegacyClusteringRedirectService.databaseLegacyClusterUriPattern( getConfig() ) );
        }
        return result;
    }
}
