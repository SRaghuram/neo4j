/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;

public class NativeAndCredentialsOnlyIT extends EnterpriseAuthenticationTestBase
{
    @Override
    protected Map<Setting<?>, String> getSettings()
    {
        return Collections.singletonMap( SecuritySettings.auth_providers, "native,plugin-TestCredentialsOnlyPlugin" );
    }

    @Test
    public void shouldAuthenticateWithCredentialsOnlyPlugin()
    {
        assertAuth( "", "BASE64-ENC-PASSWORD", "plugin-TestCredentialsOnlyPlugin" );
    }
}
