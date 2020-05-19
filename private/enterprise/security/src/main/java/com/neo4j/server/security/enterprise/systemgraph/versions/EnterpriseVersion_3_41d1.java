/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent.LATEST_VERSION;

public class EnterpriseVersion_3_41d1 extends SupportedEnterpriseVersion
{
    public EnterpriseVersion_3_41d1( Log log )
    {
        super( 3, "Neo4j 4.1.0-Drop01", log );
    }

    @Override
    public boolean detected( Transaction tx )
    {
        return nodesWithLabelExist( tx, DATABASE_ALL_LABEL ) &&
               nodesWithLabelExist( tx, DATABASE_DEFAULT_LABEL ) &&
               componentNotInVersionNode( tx );
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

    // INITIALIZATION

    @Override
    public void setUpDefaultPrivileges( Transaction tx )
    {
        super.setUpDefaultPrivileges( tx );
    }

    @Override
    public void assignDefaultPrivileges( Node role, String predefinedRole )
    {
        super.assignDefaultPrivileges( role, predefinedRole );
    }

    // UPGRADE

    @Override
    public void upgradeSecurityGraph( Transaction tx, KnownEnterpriseSecurityComponentVersion latest )
    {
        assert latest.version == LATEST_VERSION;
        log.info( String.format( "Upgrading security model from %s by adding version information", this.description ) );
        // Upgrade from 4.1.0-Drop01 to 4.1.x, which means add the Version node
        setVersionProperty( tx, latest.version );
    }

    // RUNTIME

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase ) throws UnsupportedOperationException
    {
        switch ( action )
        {
        case SET_USER_STATUS:
        case SET_PASSWORDS:

        case CREATE_ELEMENT:
        case DELETE_ELEMENT:
        case SET_LABEL:
        case REMOVE_LABEL:
        case SET_PROPERTY:
        case MERGE:

        case GRAPH_ACTIONS:
            throw unsupportedAction();

        default:
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
