/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;

import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;

public class InClusterLoginContext implements EnterpriseLoginContext
{

    private final AuthSubject authSubject;
    private final Set<String> roles;
    private final String defaultDatabase;
    private final Supplier<Set<ResourcePrivilege>> permissionsSupplier;

    public InClusterLoginContext( String username, Set<String> roles, String defaultDatabase, Supplier<Set<ResourcePrivilege>> permissionsSupplier )
    {
        this.authSubject = new AuthSubjectImpl( username );
        this.roles = roles;
        this.defaultDatabase = defaultDatabase;
        this.permissionsSupplier = permissionsSupplier;
    }

    @Override
    public Set<String> roles()
    {
        return roles;
    }

    @Override
    public AuthSubject subject()
    {
        return authSubject;
    }

    @Override
    public EnterpriseSecurityContext authorize( IdLookup idLookup, String dbName )
    {
        StandardAccessModeBuilder accessModeBuilder = new StandardAccessModeBuilder( true, false, roles, idLookup, dbName, defaultDatabase );

        Set<ResourcePrivilege> privileges = permissionsSupplier.get();

        StandardAccessMode mode = StandardEnterpriseLoginContext.mode( accessModeBuilder, privileges, dbName, defaultDatabase, authSubject.username(), roles );
        return new EnterpriseSecurityContext( authSubject, mode, mode.roles(), mode.getAdminAccessMode() );
    }

    private class AuthSubjectImpl implements AuthSubject
    {
        private String username;
        private AuthenticationResult authenticationResult;

        AuthSubjectImpl( String username )
        {
            this.username = username;
            this.authenticationResult = SUCCESS;
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return authenticationResult;
        }

        @Override
        public void setPasswordChangeNoLongerRequired()
        {

        }

        @Override
        public String username()
        {
            return username;
        }

        @Override
        public boolean hasUsername( String username )
        {
            return Objects.equals( this.username, username );
        }
    }
}
