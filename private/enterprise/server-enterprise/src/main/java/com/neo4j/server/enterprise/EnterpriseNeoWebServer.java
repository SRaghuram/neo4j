/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.server.enterprise.modules.EnterpriseAuthorizationModule;
import com.neo4j.server.rest.DatabaseRoleInfoServerModule;
import com.neo4j.server.rest.LegacyManagementModule;
import com.neo4j.server.rest.causalclustering.CausalClusteringService;
import com.neo4j.server.rest.causalclustering.LegacyCausalClusteringRedirectService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.CommunityNeoWebServer;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static com.neo4j.server.rest.EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs;

public class EnterpriseNeoWebServer extends CommunityNeoWebServer
{
    public EnterpriseNeoWebServer( DatabaseManagementService managementService, Dependencies globalDependencies, Config config,
            LogProvider userLogProvider, DatabaseInfo databaseInfo )
    {
        super( managementService, globalDependencies, config, userLogProvider, databaseInfo );
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
        // ConnectorPortRegister isn't available until runtime, so defer loading until then
        Supplier<DiscoverableURIs> discoverableURIs  = () -> enterpriseDiscoverableURIs(
                getConfig(), getGlobalDependencies().resolveDependency( ConnectorPortRegister.class ) );
        return new DBMSModule( webServer, getConfig(), discoverableURIs, userLogProvider );
    }

    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        List<ServerModule> modules = new ArrayList<>();
        modules.add( new DatabaseRoleInfoServerModule( webServer, getConfig() ) );
        modules.add( new LegacyManagementModule( webServer, getConfig() ) );
        super.createServerModules().forEach( modules::add );
        return modules;
    }

    @Override
    protected List<Pattern> getUriWhitelist()
    {
        var result = new ArrayList<>( super.getUriWhitelist() );
        if ( !getConfig().get( CausalClusteringSettings.status_auth_enabled ) )
        {
            result.add( CausalClusteringService.databaseClusterUriPattern( getConfig() ) );
            result.add( LegacyCausalClusteringRedirectService.databaseLegacyClusterUriPattern( getConfig() ) );
        }
        return result;
    }
}
