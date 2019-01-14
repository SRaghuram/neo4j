/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.junit.Test;

import org.neo4j.driver.v1.Driver;

public abstract class AuthTestBase extends EnterpriseAuthenticationTestBase
{
    static final String NONE_USER = "smith";
    static final String READ_USER = "neo";
    static final String WRITE_USER = "tank";
    static final String PROC_USER = "jane";

    @Test
    public void shouldLoginWithCorrectInformation()
    {
        assertAuth( READ_USER, getPassword() );
        assertAuth( READ_USER, getPassword() );
    }

    @Test
    public void shouldFailLoginWithIncorrectCredentials()
    {
        assertAuthFail( READ_USER, "WRONG" );
        assertAuthFail( READ_USER, "ALSO WRONG" );
    }

    @Test
    public void shouldFailLoginWithInvalidCredentialsFollowingSuccessfulLogin()
    {
        assertAuth( READ_USER, getPassword() );
        assertAuthFail( READ_USER, "WRONG" );
    }

    @Test
    public void shouldLoginFollowingFailedLogin()
    {
        assertAuthFail( READ_USER, "WRONG" );
        assertAuth( READ_USER, getPassword() );
    }

    @Test
    public void shouldGetCorrectAuthorizationNoPermission()
    {
        try ( Driver driver = connectDriver( NONE_USER, getPassword() ) )
        {
            assertReadFails( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldGetCorrectAuthorizationReaderUser()
    {
        try ( Driver driver = connectDriver( READ_USER, getPassword() ) )
        {
            assertReadSucceeds( driver );
            assertWriteFails( driver );
        }
    }

    @Test
    public void shouldGetCorrectAuthorizationWriteUser()
    {
        try ( Driver driver = connectDriver( WRITE_USER, getPassword() ) )
        {
            assertReadSucceeds( driver );
            assertWriteSucceeds( driver );
        }
    }

    @Test
    public void shouldGetCorrectAuthorizationAllowedProcedure()
    {
        try ( Driver driver = connectDriver( PROC_USER, getPassword() ) )
        {
            assertProcSucceeds( driver );
            assertReadFails( driver );
            assertWriteFails( driver );
        }
    }

    protected abstract String getPassword();
}
