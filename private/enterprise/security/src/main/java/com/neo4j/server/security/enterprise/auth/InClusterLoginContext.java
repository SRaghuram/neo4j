/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;

import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
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
        StandardAccessMode.Builder accessModeBuilder = new StandardAccessMode.Builder( true, false, roles, idLookup, dbName, defaultDatabase );

        Set<ResourcePrivilege> privileges = permissionsSupplier.get();
        boolean isDefault = dbName.equals( defaultDatabase );
        for ( ResourcePrivilege privilege : privileges )
        {
            if ( privilege.appliesTo( dbName ) || isDefault && privilege.appliesToDefault() )
            {
                accessModeBuilder.addPrivilege( privilege );
            }
        }
        if ( dbName.equals( SYSTEM_DATABASE_NAME ) )
        {
            accessModeBuilder.withAccess();
        }

        StandardAccessMode mode = accessModeBuilder.build();
        if ( !mode.allowsAccess() )
        {
            throw mode.onViolation(
                    String.format( "Database access is not allowed for user '%s' with roles %s.", authSubject.username(), new TreeSet<>( roles ).toString() ) );
        }

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
        public void logout()
        {
            username = null;
            authenticationResult = FAILURE;
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
