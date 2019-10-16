/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.string.UTF8;

import static org.neo4j.procedure.Mode.DBMS;

public class AuraUserManagementProcedures
{
    @Context
    public EnterpriseUserManager userManager;

    @Admin
    @Description( "Create a new user." )
    @Procedure( name = "dbms.security.createUser", mode = DBMS )
    public void createUser( @Name( "username" ) String username, @Name( "password" ) String password,
            @Name( value = "requirePasswordChange", defaultValue = "true" ) boolean requirePasswordChange )
            throws InvalidArgumentsException, IOException
    {
        // TODO: Deprecate this and create a new procedure that takes password as a byte[]
        userManager.newUser( username, password != null ? UTF8.encode( password ) : null, requirePasswordChange );
        userManager.addRoleToUser( PredefinedRoles.ADMIN, username );
    }
}
