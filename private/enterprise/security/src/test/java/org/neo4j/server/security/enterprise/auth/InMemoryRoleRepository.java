/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import java.util.ArrayList;

import org.neo4j.server.security.auth.ListSnapshot;

import static org.neo4j.server.security.auth.ListSnapshot.FROM_MEMORY;

/** A role repository implementation that just stores roles in memory */
public class InMemoryRoleRepository extends AbstractRoleRepository
{
    @Override
    protected void persistRoles()
    {
        // Nothing to do
    }

    @Override
    protected ListSnapshot<RoleRecord> readPersistedRoles()
    {
        return null;
    }

    @Override
    public ListSnapshot<RoleRecord> getPersistedSnapshot()
    {
        return new ListSnapshot<>( lastLoaded.get(), new ArrayList<>( roles ), FROM_MEMORY );
    }
}
