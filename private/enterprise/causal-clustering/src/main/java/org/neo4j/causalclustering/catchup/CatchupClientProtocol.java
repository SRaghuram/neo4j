/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

public class CatchupClientProtocol extends Protocol<CatchupClientProtocol.State>
{
    public CatchupClientProtocol()
    {
        super( State.MESSAGE_TYPE );
    }

    public enum State
    {
        MESSAGE_TYPE,
        STORE_ID,
        CORE_SNAPSHOT,
        TX_PULL_RESPONSE,
        STORE_COPY_FINISHED,
        TX_STREAM_FINISHED,
        FILE_HEADER,
        FILE_CONTENTS,
        PREPARE_STORE_COPY_RESPONSE,
        INDEX_SNAPSHOT_RESPONSE,
        ERROR_RESPONSE
    }
}
