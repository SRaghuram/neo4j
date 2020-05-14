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

/**
 * Version 4 of the security model is identical to version 3, but with the Version node now existing and containing the correct version information.
 */
public class EnterpriseVersion_4_41d2 extends SupportedEnterpriseVersion
{
    public EnterpriseVersion_4_41d2( Log log )
    {
        super( LATEST_VERSION, "Neo4j 4.1.0-Drop02", log, true );
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
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase ) throws UnsupportedOperationException
    {
        // Current version supports all current commands
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
