/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.configuration;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;

import static com.neo4j.server.security.enterprise.configuration.SecuritySettings.authentication_providers;
import static com.neo4j.server.security.enterprise.configuration.SecuritySettings.authorization_providers;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SecuritySettingsMigratorTest
{
    @Test
    void testAuthProviderMigrator()
    {
        var config = Config.newBuilder().setRaw( Map.of( "dbms.security.auth_provider", "ldap" ) ).build();
        List<String> authentication = config.get( authentication_providers );
        List<String> authorization = config.get( authorization_providers );
        List<String> expected = List.of( "ldap" );
        assertEquals( expected, authentication );
        assertEquals( expected, authorization );
    }
}
