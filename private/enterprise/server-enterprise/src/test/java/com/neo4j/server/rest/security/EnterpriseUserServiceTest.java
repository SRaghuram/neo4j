/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import com.neo4j.server.security.enterprise.auth.MultiRealmAuthManagerRule;
import com.neo4j.server.security.enterprise.auth.ShiroSubject;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.server.rest.dbms.UserServiceTest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EnterpriseUserServiceTest extends UserServiceTest
{
    @Rule
    public MultiRealmAuthManagerRule authManagerRule = new MultiRealmAuthManagerRule();

    @Override
    protected UserManagerSupplier setupUserManagerSupplier()
    {
        return authManagerRule.getManager();
    }

    @Override
    protected LoginContext setupSubject()
    {
        ShiroSubject shiroSubject = mock( ShiroSubject.class );
        when( shiroSubject.getPrincipal() ).thenReturn( USERNAME );
        return authManagerRule.makeLoginContext( shiroSubject );
    }

    @Test
    public void shouldLogPasswordChange() throws Exception
    {
        shouldChangePasswordAndReturnSuccess();

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( USERNAME, "changed password" );
    }

    @Test
    public void shouldLogFailedPasswordChange() throws Exception
    {
        shouldReturn422IfPasswordIdentical();

        MultiRealmAuthManagerRule.FullSecurityLog fullLog = authManagerRule.getFullSecurityLog();
        fullLog.assertHasLine( USERNAME, "tried to change password: Old password and new password cannot be the same." );
    }
}
