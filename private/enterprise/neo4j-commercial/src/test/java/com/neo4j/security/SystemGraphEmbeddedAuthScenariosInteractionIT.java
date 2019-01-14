/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import java.util.Map;

import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.server.security.enterprise.auth.AuthScenariosInteractionTestBase;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;

public class SystemGraphEmbeddedAuthScenariosInteractionIT extends AuthScenariosInteractionTestBase<EnterpriseLoginContext>
{
    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<String, String> config )
            throws Throwable
    {
        return new SystemGraphEmbeddedInteraction( config, testDirectory );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Integer )
        {
            return ((Integer) obj).longValue();
        }
        else
        {
            return obj;
        }
    }
}
