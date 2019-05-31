/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import org.junit.Test;

import java.util.List;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public abstract class AuthTestBase extends EnterpriseAuthenticationTestBase
{
    static final String NONE_USER = "smith";
    static final String READ_USER = "neo";
    static final String WRITE_USER = "tank";
    static final String PROC_USER = "jane";
    static final String ADMIN_USER = "adminUser";

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

    @Test
    public void shouldShowDatabasesOnSystem()
    {
        try ( Driver driver = connectDriver( ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( t -> t.withDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                List<Record> records = session.run( "SHOW DATABASES" ).list();
                assertThat( records.size(), equalTo( 2 ) );
            }
        }
    }

    @Test
    public void shouldFailNicelyOnCreateDuplicateUser()
    {
        try ( Driver driver = connectDriver( ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( t -> t.withDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "CREATE USER " + ADMIN_USER + " SET PASSWORD 'foo'" ).list();
                fail( "should have gotten exception" );
            }
            catch ( ClientException ce )
            {
                assertThat( ce.getMessage(), equalTo( "The specified user '" + ADMIN_USER + "' already exists." ) );
            }
        }
    }

    @Test
    public void shouldFailNicelyOnGrantToNonexistentRole()
    {
        try ( Driver driver = connectDriver( ADMIN_USER, getPassword() ) )
        {
            try ( Session session = driver.session( t -> t.withDatabase( SYSTEM_DATABASE_NAME ) ) )
            {
                session.run( "GRANT ROLE none TO " + READ_USER ).list();
                fail( "should have gotten exception" );
            }
            catch ( ClientException ce )
            {
                assertThat( ce.getMessage(), equalTo( "Cannot grant non-existent role 'none' to user '" + READ_USER + "'" ) );
            }
        }
    }

    protected abstract String getPassword();
}
