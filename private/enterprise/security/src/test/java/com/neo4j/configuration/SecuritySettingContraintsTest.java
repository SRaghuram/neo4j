/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.configuration.Config;

import static com.neo4j.configuration.SecuritySettings.ldap_authentication_user_dn_template;
import static com.neo4j.configuration.SecuritySettings.ldap_authorization_group_membership_attribute_names;
import static com.neo4j.configuration.SecuritySettings.ldap_authorization_group_to_role_mapping;
import static com.neo4j.configuration.SecuritySettings.ldap_authorization_user_search_base;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecuritySettingContraintsTest
{
    @Test
    void groupToRoleMappingShouldBeAbleToBeEmpty()
    {
        Config config = Config.defaults();
        config.setDynamic( ldap_authorization_group_to_role_mapping, "", getClass().getSimpleName() );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveMultipleRoles()
    {
        Config config = Config.defaults();
        config.setDynamic( ldap_authorization_group_to_role_mapping, "group=role1,role2,role3", getClass().getSimpleName() );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveMultipleGroups()
    {
        Config config = Config.defaults();
        config.setDynamic( ldap_authorization_group_to_role_mapping, "group1=role1;group2=role2,role3;group3=role4", getClass().getSimpleName() );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveQuotedKeysAndWhitespaces()
    {
        Config config = Config.defaults();
        config.setDynamic( ldap_authorization_group_to_role_mapping, "'group1' = role1;\t \"group2\"\n=\t role2,role3 ;  gr oup3= role4\n ;'group4 '= ; g =r",
                getClass().getSimpleName() );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveTrailingSemicolons()
    {
        Config config = Config.defaults();
        config.setDynamic( ldap_authorization_group_to_role_mapping, "group=role;;", getClass().getSimpleName() );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveTrailingCommas()
    {
        Config config = Config.defaults();
        config.setDynamic( ldap_authorization_group_to_role_mapping, "group=role1,role2,role3,,,", getClass().getSimpleName() );
    }

    @Test
    void groupToRoleMappingShouldBeAbleToHaveNoRoles()
    {
        Config config = Config.defaults();
        config.setDynamic( ldap_authorization_group_to_role_mapping, "group=,", getClass().getSimpleName() );
    }

    @Test
    void groupToRoleMappingShouldNotBeAbleToHaveInvalidFormat()
    {
        Config config = Config.defaults();
        assertThatThrownBy( () -> config.setDynamic( ldap_authorization_group_to_role_mapping, "group", getClass().getSimpleName() ) )
                .isInstanceOf( IllegalArgumentException.class )
                .hasMessageEndingWith( "'group' could not be parsed" );
    }

    @Test
    void groupToRoleMappingShouldNotBeAbleToHaveEmptyGroupName()
    {
        Config config = Config.defaults();
        assertThatThrownBy( () -> config.setDynamic( ldap_authorization_group_to_role_mapping, "=role", getClass().getSimpleName() ) )
                .isInstanceOf( IllegalArgumentException.class )
                .hasMessageEndingWith( "'=role' could not be parsed" );
    }

    @Test
    void UserDnTemplateMustContainSubstitutionCharacter()
    {
        Config config = Config.defaults();
        assertThatThrownBy(
                () -> config.setDynamic( ldap_authentication_user_dn_template, "uid=neo4j,ou=users,dc=example,dc=com", getClass().getSimpleName() ) )
                .isInstanceOf( IllegalArgumentException.class )
                .hasMessageEndingWith( "uid=neo4j,ou=users,dc=example,dc=com' must contain '{0}" );
    }

    @Test
    void UserSearchBaseCannotBeEmpty()
    {
        Config config = Config.defaults();
        assertThatThrownBy( () -> config.setDynamic( ldap_authorization_user_search_base, "", getClass().getSimpleName() ) )
                .isInstanceOf( IllegalArgumentException.class )
                .hasMessageEndingWith( "Can not be empty" );
    }

    @Test
    void GroupMembershipAttributeCannotBeEmpty()
    {
        Config config = Config.defaults();
        assertThatThrownBy( () ->
                config.setDynamic( ldap_authorization_group_membership_attribute_names, List.of(), getClass().getSimpleName() ) )
                .isInstanceOf( IllegalArgumentException.class )
                .hasMessageEndingWith( "Can not be empty" );
    }
}
