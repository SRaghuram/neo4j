/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.server.security.enterprise.auth.ConfiguredAuthScenariosInteractionTestBase;
import com.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;

import java.util.Map;

public class SystemGraphEmbeddedConfiguredAuthScenariosInteractionIT extends ConfiguredAuthScenariosInteractionTestBase<CommercialLoginContext>
{
    @Override
    protected NeoInteractionLevel<CommercialLoginContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new SystemGraphEmbeddedInteraction( config, testDirectory );
    }

    @Override
    protected String internalSecurityName()
    {
        return SecuritySettings.SYSTEM_GRAPH_REALM_NAME;
    }
}
