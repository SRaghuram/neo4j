/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

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
        PREPARE_STORE_COPY,
        GET_DATABASE_ID,
        GET_ALL_DATABASE_IDS,
        GET_METADATA,
        GET_INFO;
    }
}
