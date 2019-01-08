/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.AuthScenariosInteractionTestBase;
import com.neo4j.server.security.enterprise.auth.NeoInteractionLevel;

import java.util.Map;

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
