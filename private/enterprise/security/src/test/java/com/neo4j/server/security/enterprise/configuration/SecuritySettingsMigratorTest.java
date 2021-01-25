/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.configuration;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.configuration.Config;

import static com.neo4j.configuration.SecuritySettings.authentication_providers;
import static com.neo4j.configuration.SecuritySettings.authorization_providers;
import static com.neo4j.configuration.SecuritySettings.ldap_authentication_use_attribute;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void testSamAccountNameMigratorOverwriteDefaultValue()
    {
        var config = Config.newBuilder().setRaw( Map.of( "dbms.security.ldap.authentication.use_samaccountname", "true" ) ).build();
        boolean useAttribute = config.get( ldap_authentication_use_attribute );
        assertTrue( useAttribute );
    }

    @Test
    void testSamAccountNameMigratorShouldNotOverwriteExplicitValue()
    {
        var config = Config.newBuilder()
                .setRaw( Map.of( "dbms.security.ldap.authentication.use_samaccountname", "false",
                        "dbms.security.ldap.authentication.search_for_attribute", "true" ) ).build();

        boolean useAttribute = config.get( ldap_authentication_use_attribute );
        assertTrue( useAttribute );
    }
}
