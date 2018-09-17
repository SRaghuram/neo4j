/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security.configuration;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.configuration.Config;

import static com.neo4j.security.configuration.CommercialSecuritySettings.isSystemDatabaseEnabled;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.SYSTEM_GRAPH_REALM_NAME;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.auth_provider;

class CommercialSecuritySettingsTest
{
    @Test
    void securityDatabaseDisableByDefault()
    {
        assertFalse( isSystemDatabaseEnabled( defaults() ) );
    }

    @Test
    void systemGraphRealmUsageEnableSecurityDatabase()
    {
        Config config = defaults( auth_provider, SYSTEM_GRAPH_REALM_NAME );
        assertTrue( isSystemDatabaseEnabled( config ) );
    }
}
