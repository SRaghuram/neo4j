/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.logging;

import org.neo4j.causalclustering.core.consensus.RaftMessages;

public class NullMessageLogger<MEMBER> implements MessageLogger<MEMBER>
{
    @Override
    public <M extends RaftMessages.RaftMessage> void logOutbound( MEMBER me, M message, MEMBER remote )
    {

    }

    @Override
    public <M extends RaftMessages.RaftMessage> void logInbound( MEMBER remote, M message, MEMBER me )
    {
    }
}

