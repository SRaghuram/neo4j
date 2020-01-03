/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.logging;

import static com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;

public class NullRaftMessageLogger<MEMBER> implements RaftMessageLogger<MEMBER>
{
    @Override
    public void logOutbound( MEMBER me, RaftMessage message, MEMBER remote )
    {
    }

    @Override
    public void logInbound( MEMBER remote, RaftMessage message, MEMBER me )
    {
    }
}

