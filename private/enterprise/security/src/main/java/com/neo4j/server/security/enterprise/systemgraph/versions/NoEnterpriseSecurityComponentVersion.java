/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.neo4j.causalclustering.catchup.v4.metadata.DatabaseSecurityCommands;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.logging.NullLog;

import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_UNKNOWN_VERSION;

public class NoEnterpriseSecurityComponentVersion extends KnownEnterpriseSecurityComponentVersion
{
    public NoEnterpriseSecurityComponentVersion()
    {
        super( ENTERPRISE_SECURITY_UNKNOWN_VERSION, NullLog.getInstance() );
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantDefaultPrivileges( Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void upgradeSecurityGraph( Transaction tx, int fromVersion )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase, Segment segment ) throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public DatabaseSecurityCommands getBackupCommands( Transaction tx, String databaseName, boolean saveUsers, boolean saveRoles )
    {
        throw new UnsupportedOperationException();
    }
}
