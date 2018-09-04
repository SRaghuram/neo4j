/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import java.util.Map;

import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.server.security.enterprise.auth.ConfiguredAuthScenariosInteractionTestBase;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;

public class SystemGraphEmbeddedConfiguredAuthScenariosInteractionIT extends ConfiguredAuthScenariosInteractionTestBase<EnterpriseLoginContext>
{
    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new SystemGraphEmbeddedInteraction( config, testDirectory );
    }

    @Override
    protected String internalSecurityName()
    {
        return "system-graph";
    }
}
