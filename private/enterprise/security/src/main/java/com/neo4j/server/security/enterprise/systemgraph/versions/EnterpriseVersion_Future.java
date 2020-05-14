/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;

/**
 * This class represents all unrecognised future versions of the security model. This can occur in a clustered environment when
 * the leader has been upgraded to a newer version of Neo4j, but followers have not. When this case is detected, all security
 * authorization should be disabled and the results of the dbms.upgradeStatus procedure should explain the problem.
 */
public class EnterpriseVersion_Future extends KnownEnterpriseSecurityComponentVersion
{
    private final KnownEnterpriseSecurityComponentVersion latestVersion;

    public EnterpriseVersion_Future( Log log, KnownEnterpriseSecurityComponentVersion latestKnownVersion )
    {
        super( Integer.MIN_VALUE, "Unrecognized future version", log );
        this.latestVersion = latestKnownVersion;
        this.getStatus();
    }

    public SystemGraphComponent.Status getStatus()
    {
        return SystemGraphComponent.Status.UNSUPPORTED_FUTURE;
    }

    @Override
    public UnsupportedOperationException unsupported()
    {
        return new UnsupportedOperationException(
                String.format( "System graph version for component '%s' is newer than the most recent supported '%s'", componentVersionProperty,
                        latestVersion.description ) );
    }

    @Override
    public boolean detected( Transaction tx )
    {
        return getVersion( tx ) > latestVersion.version;
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx )
    {
        throw unsupported();
    }

    @Override
    public void assignDefaultPrivileges( Node role, String predefinedRole )
    {
        throw unsupported();
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase ) throws UnsupportedOperationException
    {
        throw unsupported();
    }
}