/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security.configuration;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.configuration.Config;

import static com.neo4j.security.configuration.CommercialSecuritySettings.isSystemDatabaseEnabled;
import static com.neo4j.security.configuration.CommercialSecuritySettings.system_graph_authentication_enabled;
import static com.neo4j.security.configuration.CommercialSecuritySettings.system_graph_authorization_enabled;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.configuration.Settings.TRUE;

class CommercialSecuritySettingsTest
{

    @Test
    void securityDatabaseDisableByDefault()
    {
        assertFalse( isSystemDatabaseEnabled( defaults() ) );
    }

    @Test
    void graphAuthenticationEnableSecurityDatabase()
    {
        Config config = defaults( system_graph_authentication_enabled, TRUE );
        assertTrue( isSystemDatabaseEnabled( config ) );
    }

    @Test
    void graphAuthorizationEnableSecurityDatabase()
    {
        Config config = defaults( system_graph_authorization_enabled, TRUE );
        assertTrue( isSystemDatabaseEnabled( config ) );
    }
}
