/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.modules;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.http.cypher.LegacyTransactionRedirectService;
import org.neo4j.server.rest.web.CorsFilter;
import org.neo4j.server.web.WebServer;

import static java.util.Collections.singletonList;
import static org.neo4j.server.configuration.ServerSettings.http_access_control_allow_origin;

/**
 * Mounts the legacy transaction module.
 */
public class LegacyTransactionModule implements ServerModule
{
    private final Config config;
    private final WebServer webServer;
    private final LogProvider logProvider;

    public LegacyTransactionModule( WebServer webServer, Config config, LogProvider logProvider )
    {
        this.webServer = webServer;
        this.config = config;
        this.logProvider = logProvider;
    }

    @Override
    public void start()
    {
        webServer.addFilter( new CorsFilter( logProvider, config.get( http_access_control_allow_origin ) ), "/*" );
        webServer.addJAXRSClasses( jaxRsClasses(), mountPoint(), null );
    }

    private List<Class<?>> jaxRsClasses()
    {
        return singletonList( LegacyTransactionRedirectService.class );
    }

    @Override
    public void stop()
    {
        webServer.removeJAXRSClasses( jaxRsClasses(), mountPoint() );
    }

    private String mountPoint()
    {
        return config.get( ServerSettings.rest_api_path ).getPath();
    }
}
