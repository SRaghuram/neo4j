/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.NullLog;

public class NoEnterpriseSecurityComponentVersion extends KnownEnterpriseSecurityComponentVersion
{
    public static final int VERSION = -1;

    public NoEnterpriseSecurityComponentVersion()
    {
        super( VERSION, String.format( "no '%s' graph found", EnterpriseSecurityGraphComponent.COMPONENT ), NullLog.getInstance() );
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
}
