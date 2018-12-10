/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;

import java.util.Set;

public interface RaftGroup<MEMBER> extends ReplicatedContent
{
    Set<MEMBER> getMembers();

    interface Builder<MEMBER>
    {
        RaftGroup<MEMBER> build( Set<MEMBER> members );
    }
}
