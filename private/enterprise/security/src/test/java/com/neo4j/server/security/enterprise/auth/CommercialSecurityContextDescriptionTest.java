/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialSecurityContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

public class CommercialSecurityContextDescriptionTest
{
    @Rule
    public MultiRealmAuthManagerRule authManagerRule = new MultiRealmAuthManagerRule();

    private EnterpriseUserManager manager;
    private final LoginContext.IdLookup token = LoginContext.IdLookup.EMPTY;

    @Before
    public void setUp() throws Throwable
    {
        authManagerRule.getManager().start();
        manager = authManagerRule.getManager().getUserManager();
        manager.newUser( "mats", password( "foo" ), false );
    }

    @Test
    public void shouldMakeNiceDescriptionWithoutRoles() throws Exception
    {
        assertThat( context().description(), equalTo( "user 'mats' with no roles" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionWithRoles() throws Exception
    {
        manager.newRole( "role1", "mats" );
        manager.addRoleToUser( PUBLISHER, "mats" );

        assertThat( context().description(), equalTo( "user 'mats' with roles [publisher,role1]" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionWithMode() throws Exception
    {
        manager.newRole( "role1", "mats" );
        manager.addRoleToUser( PUBLISHER, "mats" );

        CommercialSecurityContext modified = context().withMode( AccessMode.Static.CREDENTIALS_EXPIRED );
        assertThat( modified.description(), equalTo( "user 'mats' with CREDENTIALS_EXPIRED" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionRestricted() throws Exception
    {
        manager.newRole( "role1", "mats" );
        manager.addRoleToUser( PUBLISHER, "mats" );

        CommercialSecurityContext context = context();
        CommercialSecurityContext restricted =
                context.withMode( new RestrictedAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "user 'mats' with roles [publisher,role1] restricted to READ" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionOverridden() throws Exception
    {
        manager.newRole( "role1", "mats" );
        manager.addRoleToUser( PUBLISHER, "mats" );

        CommercialSecurityContext context = context();
        CommercialSecurityContext overridden =
                context.withMode( new OverriddenAccessMode( context.mode(), AccessMode.Static.READ ) );
        assertThat( overridden.description(), equalTo( "user 'mats' with roles [publisher,role1] overridden by READ" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionAuthDisabled()
    {
        CommercialSecurityContext disabled = CommercialSecurityContext.AUTH_DISABLED;
        assertThat( disabled.description(), equalTo( "AUTH_DISABLED with FULL" ) );
    }

    @Test
    public void shouldMakeNiceDescriptionAuthDisabledAndRestricted()
    {
        CommercialSecurityContext disabled = CommercialSecurityContext.AUTH_DISABLED;
        CommercialSecurityContext restricted =
                disabled.withMode( new RestrictedAccessMode( disabled.mode(), AccessMode.Static.READ ) );
        assertThat( restricted.description(), equalTo( "AUTH_DISABLED with FULL restricted to READ" ) );
    }

    private CommercialSecurityContext context() throws InvalidAuthTokenException, KernelException
    {
        return authManagerRule.getManager().login( authToken( "mats", "foo" ) )
                .authorize( token, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }
}
