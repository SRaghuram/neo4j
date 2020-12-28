/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.log.SecurityLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;

import static org.neo4j.kernel.impl.security.User.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.server.security.systemgraph.SystemGraphRealmHelper.IS_SUSPENDED;

@SuppressWarnings( "WeakerAccess" )
public class AuthProceduresBase
{
    @Context
    public SecurityContext securityContext;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public Transaction transaction;

    @Context
    public SecurityLog securityLog;

    // ----------------- helpers ---------------------

    public static class StringResult
    {
        public final String value;

        StringResult( String value )
        {
            this.value = value;
        }
    }

    protected UserResult userResultForSubject()
    {
        String username = securityContext.subject().username();
        boolean changeReq = securityContext.subject().getAuthenticationResult().equals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED );
        return new UserResult( username, securityContext.roles(), changeReq, false );
    }

    public static class UserResult
    {
        public final String username;
        public final List<String> roles;
        public final List<String> flags;

        UserResult( String username, Collection<String> roles, boolean changeRequired, boolean suspended )
        {
            this.username = username;
            this.roles = new ArrayList<>();
            this.roles.addAll( roles );
            this.flags = new ArrayList<>();
            if ( changeRequired )
            {
                flags.add( PASSWORD_CHANGE_REQUIRED );
            }
            if ( suspended )
            {
                flags.add( IS_SUSPENDED );
            }
        }
    }

    public static class RoleResult
    {
        public final String role;
        public final List<String> users;

        RoleResult( String role, Set<String> users )
        {
            this.role = role;
            this.users = new ArrayList<>();
            this.users.addAll( users );
        }
    }
}
