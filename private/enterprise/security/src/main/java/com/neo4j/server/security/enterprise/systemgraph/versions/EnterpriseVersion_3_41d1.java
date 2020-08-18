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

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent.LATEST_VERSION;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;

public class EnterpriseVersion_3_41d1 extends SupportedEnterpriseVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseVersion_3_41d1( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( 3, VERSION_41D1, log );
        this.previous = previous;
    }

    @Override
    public boolean detected( Transaction tx )
    {
        return nodesWithLabelExist( tx, DATABASE_ALL_LABEL ) &&
               nodesWithLabelExist( tx, DATABASE_DEFAULT_LABEL ) &&
               componentNotInVersionNode( tx );
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
        Node publicRole = tx.findNode( ROLE_LABEL, "name", PUBLIC );
        grantExecutePrivilegeTo( tx, publicRole );
    }

    // RUNTIME

    @Override
    boolean supportsUpdateAction( PrivilegeAction action )
    {
        switch ( action )
        {
        // User management
        case CREATE_USER:
        case DROP_USER:
        case SHOW_USER:
        case ALTER_USER:
        case USER_MANAGEMENT:

        // Database management
        case CREATE_DATABASE:
        case DROP_DATABASE:
        case DATABASE_MANAGEMENT:

        // Privilege management
        case SHOW_PRIVILEGE:
        case ASSIGN_PRIVILEGE:
        case REMOVE_PRIVILEGE:
        case PRIVILEGE_MANAGEMENT:

        case DBMS_ACTIONS:

        case TRANSACTION_MANAGEMENT:
        case SHOW_TRANSACTION:
        case TERMINATE_TRANSACTION:
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
