/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;

import java.util.stream.Stream;

import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class SecurityProcedures extends AuthProceduresBase
{
    @Context
    public EnterpriseAuthManager authManager;

    @Deprecated
    @Description( "Show the current user. Deprecated by dbms.showCurrentUser." )
    @Procedure( name = "dbms.security.showCurrentUser", mode = DBMS, deprecatedBy = "dbms.showCurrentUser" )
    public Stream<UserManagementProcedures.UserResult> showCurrentUserDeprecated()
    {
        return showCurrentUser();
    }

    @Description( "Show the current user." )
    @Procedure( name = "dbms.showCurrentUser", mode = DBMS )
    public Stream<UserManagementProcedures.UserResult> showCurrentUser()
    {
        return Stream.of( userResultForSubject() );
    }

    @Admin
    @Description( "Clears authentication and authorization cache." )
    @Procedure( name = "dbms.security.clearAuthCache", mode = DBMS )
    public void clearAuthenticationCache()
    {
        authManager.clearAuthCache();
    }
}
