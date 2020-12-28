/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;

import static com.neo4j.configuration.SecuritySettings.ldap_authorization_group_to_role_mapping;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecuritySettingContraintsTest
{
    @Test
    void groupToRoleMappingShouldBeAbleToBeEmpty()
    {
        Config config = Config.defaults();
        config.set( ldap_authorization_group_to_role_mapping, "" );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveMultipleRoles()
    {
        Config config = Config.defaults();
        config.set( ldap_authorization_group_to_role_mapping, "group=role1,role2,role3" );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveMultipleGroups()
    {
        Config config = Config.defaults();
        config.set( ldap_authorization_group_to_role_mapping, "group1=role1;group2=role2,role3;group3=role4" );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveQuotedKeysAndWhitespaces()
    {
        Config config = Config.defaults();
        config.set( ldap_authorization_group_to_role_mapping, "'group1' = role1;\t \"group2\"\n=\t role2,role3 ;  gr oup3= role4\n ;'group4 '= ; g =r" );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveTrailingSemicolons()
    {
        Config config = Config.defaults();
        config.set( ldap_authorization_group_to_role_mapping, "group=role;;" );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveTrailingCommas()
    {
        Config config = Config.defaults();
        config.set( ldap_authorization_group_to_role_mapping, "group=role1,role2,role3,,," );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveNoRoles()
    {
        Config config = Config.defaults();
        config.set( ldap_authorization_group_to_role_mapping, "group=," );
    }

    @Test
    void groupToRoleMappingShouldNotBeAbleToHaveInvalidFormat()
    {
        Config config = Config.defaults();
        assertThatThrownBy( () -> config.set( ldap_authorization_group_to_role_mapping, "group" ) )
                .isInstanceOf( IllegalArgumentException.class )
                .hasMessageEndingWith( "'group' could not be parsed" );
    }

    @Test
    void groupToRoleMappingShouldNotBeAbleToHaveEmptyGroupName()
    {
        Config config = Config.defaults();
        assertThatThrownBy( () -> config.set( ldap_authorization_group_to_role_mapping, "=role" ) )
                .isInstanceOf( IllegalArgumentException.class )
                .hasMessageEndingWith( "'=role' could not be parsed" );
    }
}
