/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.logging;

import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;

public interface RaftMessageLogger<MEMBER>
{
    void logOutbound( MEMBER me, RaftMessage message, MEMBER remote );

    void logInbound( MEMBER remote, RaftMessage message, MEMBER me );
}
