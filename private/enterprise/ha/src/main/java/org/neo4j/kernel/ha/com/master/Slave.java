/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.master;

import org.neo4j.com.Response;

public interface Slave
{
    Response<Void> pullUpdates( long upToAndIncludingTxId );

    int getServerId();
}
