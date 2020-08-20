/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;

import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent.LATEST_VERSION;

/**
 * Version 4 of the security model is identical to version 3, but with the Version node now existing and containing the correct version information.
 */
public class EnterpriseVersion_4_41 extends SupportedEnterpriseVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseVersion_4_41( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( LATEST_VERSION, VERSION_41, log, true );
        this.previous = previous;
    }

    @Override
    public boolean migrationSupported()
    {
        return true;
    }

    @Override
    public boolean runtimeSupported()
    {
        return true;
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx )
    {
        super.setUpDefaultPrivileges( tx );
        this.setVersionProperty( tx, version );
    }

    @Override
    public void assignDefaultPrivileges( Node role, String predefinedRole )
    {
        super.assignDefaultPrivileges( role, predefinedRole );
    }

    @Override
    boolean supportsUpdateAction( PrivilegeAction action )
    {
        switch ( action )
        {
        // More user management
        case SET_USER_STATUS:
        case SET_PASSWORDS:

        // Fine-grained write
        case CREATE_ELEMENT:
        case DELETE_ELEMENT:
        case SET_LABEL:
        case REMOVE_LABEL:
        case SET_PROPERTY:

        case GRAPH_ACTIONS:

        case MERGE:
            return true;
        default:
            return previous.supportsUpdateAction( action );
        }
    }

    @Override
    public Set<ResourcePrivilege> getPrivilegeForRoles( Transaction tx, List<String> roleNames, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        return super.currentGetPrivilegeForRoles( tx, roleNames, privilegeCache );
    }

    @Override
    public PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        return new PrivilegeBuilder( privilegeType, action );
    }
}
