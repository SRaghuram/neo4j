/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

import org.neo4j.causalclustering.catchup.RequestMessageType;

public interface CatchUpRequest extends Message
{
    RequestMessageType messageType();
}
