/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

class ContentCodes
{
    static final byte TX_CONTENT_TYPE = 0;
    static final byte RAFT_MEMBER_SET_TYPE = 1;
    static final byte TOKEN_REQUEST_TYPE = 4;
    static final byte NEW_LEADER_BARRIER_TYPE = 5;
    static final byte LEASE_REQUEST = 6;
    static final byte DISTRIBUTED_OPERATION = 7;
    static final byte DUMMY_REQUEST = 8;
}
