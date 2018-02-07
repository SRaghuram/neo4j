/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest.security;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.server.rest.dbms.UserServiceTest;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.enterprise.auth.MultiRealmAuthManagerRule;
import org.neo4j.server.security.enterprise.auth.ShiroSubject;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EnterpriseUserServiceTest extends UserServiceTest
{
    @Rule
    public MultiRealmAuthManagerRule authManagerRule = new MultiRealmAuthManagerRule(
            userRepository,
            mock( AuthenticationStrategy.class )
    );

    @Override
    protected void setupAuthManagerAndSubject()
    {
        userManagerSupplier = authManagerRule.getManager();

        ShiroSubject shiroSubject = mock( ShiroSubject.class );
        when( shiroSubject.getPrincipal() ).thenReturn( "neo4j" );
        neo4jContext = authManagerRule.makeLoginContext( shiroSubject );
    }

    @Test
    public void shouldLogPasswordChange() throws Exception
    {
        shouldChangePasswordAndReturnSuccess();

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "neo4j", "changed password" );
    }

    @Test
    public void shouldLogFailedPasswordChange() throws Exception
    {
        shouldReturn422IfPasswordIdentical();

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( "neo4j", "tried to change password: Old password and new password cannot be the same." );
    }
}
