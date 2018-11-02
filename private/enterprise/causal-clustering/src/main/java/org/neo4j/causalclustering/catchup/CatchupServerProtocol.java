/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

public class CatchupServerProtocol extends Protocol<CatchupServerProtocol.State>
{
    public CatchupServerProtocol()
    {
        super( State.MESSAGE_TYPE );
    }

    public enum State
    {
        MESSAGE_TYPE,
        // unused
        GET_STORE,
        GET_STORE_ID,
        GET_CORE_SNAPSHOT,
        TX_PULL,
        GET_STORE_FILE,
        GET_INDEX_SNAPSHOT,
        PREPARE_STORE_COPY
    }
}
