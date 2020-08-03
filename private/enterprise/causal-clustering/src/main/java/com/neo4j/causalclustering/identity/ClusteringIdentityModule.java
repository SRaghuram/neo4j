/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import org.neo4j.dbms.identity.DefaultIdentityModule;
import org.neo4j.dbms.identity.IdentityModule;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

public abstract class ClusteringIdentityModule extends DefaultIdentityModule implements IdentityModule
{
    /**
     * This method is left here to allow us to make the changes step by step
     * But all callers should use myself() and {@link ServerId} instead in case when the whole server is addressed
     * and only use memberId() in the context of Raft with the corresponding RaftId/Databaseid
     */
    @Deprecated
    public abstract MemberId memberId();

    public abstract MemberId memberId( NamedDatabaseId namedDatabaseId );
}
