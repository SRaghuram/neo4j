/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;
import org.neo4j.server.security.systemgraph.ComponentVersion;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.1.
 */
public class EnterpriseSecurityComponentVersion_4_41 extends SupportedEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseSecurityComponentVersion_4_41( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ComponentVersion.ENTERPRISE_SECURITY_41, log );
        this.previous = previous;
    }

    // INITIALIZATION

    @Override
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        previous.setUpDefaultPrivileges( tx, privilegeStore );
        this.setVersionProperty( tx, version );
    }

    @Override
    public void grantDefaultPrivileges( Transaction tx, Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        previous.grantDefaultPrivileges( tx, role, predefinedRole, privilegeStore );
    }

    // UPGRADE

    @Override
    public void upgradeSecurityGraph( Transaction tx, int fromVersion ) throws Exception
    {
        if ( fromVersion < version )
        {
            previous.upgradeSecurityGraph( tx, fromVersion );
            // This version introduced the Version node
            this.setVersionProperty( tx, version );
        }
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
    Set<ResourcePrivilege> getTemporaryPrivileges() throws InvalidArgumentsException
    {
        return Collections
                .singleton( new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), ProcedureSegment.ALL, SpecialDatabase.ALL ) );
    }

    @Override
    public PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        return new PrivilegeBuilder( privilegeType, action );
    }
}
