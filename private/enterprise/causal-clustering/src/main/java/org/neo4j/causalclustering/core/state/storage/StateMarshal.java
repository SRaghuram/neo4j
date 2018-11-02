/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.storage;

import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

public interface StateMarshal<STATE> extends ChannelMarshal<STATE>
{
    STATE startState();

    long ordinal( STATE state );
}
