/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;

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
