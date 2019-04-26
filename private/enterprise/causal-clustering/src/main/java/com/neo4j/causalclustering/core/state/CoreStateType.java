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
    LOCK_TOKEN( 1 ),
    SESSION_TRACKER( 2 ),
    ID_ALLOCATION( 3 ),
    RAFT_CORE_STATE( 4 ),
    DB_NAME( 5 ),
    RAFT_ID( 6 ),
    CORE_MEMBER_ID( 7 ),
    RAFT_LOG( 8 ),
    RAFT_TERM( 9 ),
    RAFT_VOTE( 10 ),
    RAFT_MEMBERSHIP( 11 ),
    LAST_FLUSHED( 12 );

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
