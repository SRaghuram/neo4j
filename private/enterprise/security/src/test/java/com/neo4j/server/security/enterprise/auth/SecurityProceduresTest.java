/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.server.security.enterprise.auth.AuthProceduresBase.UserResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SecurityProceduresTest
{
    private SecurityProcedures procedures;

    @BeforeEach
    void setup()
    {
        AuthSubject subject = mock( AuthSubject.class );
        when( subject.username() ).thenReturn( "pearl" );
        when( subject.getAuthenticationResult() ).thenReturn( AuthenticationResult.SUCCESS );

        EnterpriseSecurityContext ctx = mock( EnterpriseSecurityContext.class );
        when( ctx.subject() ).thenReturn( subject );
        when( ctx.roles() ).thenReturn( Collections.singleton( "jammer" ) );

        procedures = new SecurityProcedures();
        procedures.securityContext = ctx;
    }

    @Test
    void shouldReturnSecurityContextRoles()
    {
        List<UserResult> infoList = procedures.showCurrentUser().collect( Collectors.toList() );
        assertThat( infoList.size() ).isEqualTo( 1 );

        UserResult row = infoList.get( 0 );
        assertThat( row.username ).isEqualTo( "pearl" );
        assertThat( row.roles ).contains( "jammer" );
        assertThat( row.flags ).isEmpty();
    }
}
