/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

class PrivilegeBuilder_2_40 extends PrivilegeBuilder
{
    PrivilegeBuilder_2_40( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        super( privilegeType, action, a ->
        {
            if ( a.equals( "SCHEMA" ) )
            {
                return Set.of( PrivilegeAction.INDEX, PrivilegeAction.CONSTRAINT );
            }
            return Collections.singleton( PrivilegeAction.valueOf( a ) );
        } );
    }

    @Override
    Set<ResourcePrivilege> build() throws InvalidArgumentsException
    {
        Set<ResourcePrivilege> privileges = new HashSet<>();
        for ( PrivilegeAction action : actions )
        {
            Resource actualResource = resource;
            if ( action == PrivilegeAction.WRITE )
            {
                actualResource = new Resource.GraphResource();
            }
            if ( specialDatabase != null )
            {
                privileges.add( new ResourcePrivilege( privilegeType, action, actualResource, segment, specialDatabase ) );
            }
            else
            {
                privileges.add( new ResourcePrivilege( privilegeType, action, actualResource, segment, dbName ) );
            }
        }
        return privileges;
    }
}
