/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.outcome;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.identity.MemberId;

public class Messages implements Iterable<Map.Entry<MemberId, RaftMessages.RaftMessage>>
{
    private final Map<MemberId, RaftMessages.RaftMessage> map;

    Messages( Map<MemberId, RaftMessages.RaftMessage> map )
    {
        this.map = map;
    }

    public boolean hasMessageFor( MemberId member )
    {
        return map.containsKey( member );
    }

    public RaftMessages.RaftMessage messageFor( MemberId member )
    {
        return map.get( member );
    }

    @Override
    public Iterator<Map.Entry<MemberId, RaftMessages.RaftMessage>> iterator()
    {
        return map.entrySet().iterator();
    }
}
