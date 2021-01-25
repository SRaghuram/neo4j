/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42P1;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.2.1
 */
public class EnterpriseSecurityComponentVersion_8_42P1 extends SupportedEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseSecurityComponentVersion_8_42P1( Log log, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ENTERPRISE_SECURITY_42P1, log );
        this.previous = previous;
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        previous.setUpDefaultPrivileges( tx, privilegeStore );
        this.setVersionProperty( tx, version );
    }

    @Override
    public void grantDefaultPrivileges( Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        previous.grantDefaultPrivileges( role, predefinedRole, privilegeStore );
    }

    @Override
    public void upgradeSecurityGraph( Transaction tx, int fromVersion ) throws Exception
    {
        if ( fromVersion < version )
        {
            previous.upgradeSecurityGraph( tx, fromVersion );
            mergeNode( tx, ROLE_LABEL, Map.of( "name", PredefinedRoles.PUBLIC ) );
            this.setVersionProperty( tx, version );
        }
    }

    @Override
    boolean supportsUpdateAction( PrivilegeAction action )
    {
        return previous.supportsUpdateAction( action );
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
