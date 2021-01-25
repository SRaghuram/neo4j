/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.modules;

import com.neo4j.server.rest.dbms.EnterpriseAuthorizationDisabledFilter;

import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.rest.dbms.AuthorizationDisabledFilter;
import org.neo4j.server.web.WebServer;

public class EnterpriseAuthorizationModule extends AuthorizationModule
{
    public EnterpriseAuthorizationModule( WebServer webServer,
            Supplier<AuthManager> authManager,
            LogProvider logProvider, Config config,
            List<Pattern> uriWhitelist )
    {
        super( webServer, authManager, logProvider, config, uriWhitelist );
    }

    @Override
    protected AuthorizationDisabledFilter createAuthorizationDisabledFilter()
    {
        return new EnterpriseAuthorizationDisabledFilter();
    }
}
