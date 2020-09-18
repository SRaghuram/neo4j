/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.systemgraph.BackupCommands;

import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.NullLog;
import org.neo4j.server.security.systemgraph.ComponentVersion;

public class NoEnterpriseSecurityComponentVersion extends KnownEnterpriseSecurityComponentVersion
{
    public NoEnterpriseSecurityComponentVersion()
    {
        super( ComponentVersion.ENTERPRISE_SECURITY_UNKNOWN_VERSION, NullLog.getInstance() );
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void assignDefaultPrivileges( Node role, String predefinedRole )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase ) throws UnsupportedOperationException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BackupCommands getBackupCommands( Transaction tx, String databaseName, boolean saveUsers, boolean saveRoles )
    {
        throw new UnsupportedOperationException();
    }
}
