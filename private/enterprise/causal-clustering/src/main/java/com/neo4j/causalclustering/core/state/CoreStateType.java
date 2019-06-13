/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

public enum CoreStateType
{
    DUMMY( -1 ),
    VERSION( 0 ),
//    LOCK_TOKEN( 1 ),
    SESSION_TRACKER( 1 ),
    BARRIER_TOKEN( 2 ),
    RAFT_CORE_STATE( 3 ),
    DB_NAME( 4 ),
    RAFT_ID( 5 ),
    CORE_MEMBER_ID( 6 ),
    RAFT_LOG( 7 ),
    RAFT_TERM( 8 ),
    RAFT_VOTE( 9 ),
    RAFT_MEMBERSHIP( 10 ),
    LAST_FLUSHED( 11 );

    private final int typeId;

    CoreStateType( int typeId )
    {
        this.typeId = typeId;
    }

    public int typeId()
    {
        return typeId;
    }
}
