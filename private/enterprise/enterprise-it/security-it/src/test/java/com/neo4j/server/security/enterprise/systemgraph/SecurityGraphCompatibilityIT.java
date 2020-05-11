/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.security.AuthToken;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class SecurityGraphCompatibilityIT extends SecurityGraphCompatibilityTestBase
{
    @Test
    void shouldAuthenticateOn36() throws Exception
    {
        initEnterprise( VERSION_36 );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldNotAuthorizeOn36() throws Exception
    {
        initEnterprise( VERSION_36 );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();
        assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.SUCCESS );

        // Access to System is allowed but with no other privileges
        SecurityContext securityContextSystem = loginContext.authorize( LoginContext.IdLookup.EMPTY, SYSTEM_DATABASE_NAME );
        var systemMode = securityContextSystem.mode();
        assertThat( systemMode.allowsReadPropertyAllLabels( -1 ) ).isFalse();
        assertThat( systemMode.allowsTraverseAllLabels() ).isFalse();
        assertThat( systemMode.allowsWrites() ).isFalse();

        // Access to neo4j is disallowed
        assertThrows( AuthorizationViolationException.class, () -> loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );
    }

    @Test
    void shouldAuthenticateOn40() throws Exception
    {
        initEnterprise( VERSION_40 );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldAuthorizeOn40() throws Exception
    {
        initEnterprise( VERSION_40 );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();

        assertReadWritePrivileges( loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );
    }

    @Test
    void shouldAuthenticateOn41d01() throws Exception
    {
        initEnterprise( VERSION_41D1 );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldAuthorizeOn41d01() throws Exception
    {
        initEnterprise( VERSION_41D1 );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();

        assertReadWritePrivileges( loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );
    }

    @Test
    void shouldAuthenticateOn41d02() throws Exception
    {
        initEnterprise( VERSION_41D2 );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        assertThat( loginContext.subject().getAuthenticationResult() ).isEqualTo( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
    }

    @Test
    void shouldAuthorizeOn41d02() throws Exception
    {
        initEnterprise( VERSION_41D2 );
        LoginContext loginContext = authManager.login( AuthToken.newBasicAuthToken( "neo4j", "neo4j" ) );
        loginContext.subject().setPasswordChangeNoLongerRequired();

        assertReadWritePrivileges( loginContext.authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );
    }

    private void assertReadWritePrivileges( SecurityContext securityContext )
    {
        assertThat( securityContext.mode().allowsReadPropertyAllLabels( -1 ) ).isTrue();
        assertThat( securityContext.mode().allowsTraverseAllLabels() ).isTrue();
        assertThat( securityContext.mode().allowsWrites() ).isTrue();
        assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.CREATE_INDEX ) ).isTrue();
        assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.DROP_INDEX ) ).isTrue();
        assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT ) ).isTrue();
        assertThat( securityContext.mode().allowsSchemaWrites( PrivilegeAction.DROP_CONSTRAINT ) ).isTrue();
    }
}
