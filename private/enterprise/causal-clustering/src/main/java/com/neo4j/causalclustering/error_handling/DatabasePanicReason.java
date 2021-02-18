/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

/**
 * Panic an individual database. A panic indicates that we have encountered an error that we do not expect the to be able to recover from.
 * <p>
 * Important: For the purposes of panics the DBMS and individual databases are considered to be loosely coupled. This means that a DBMS panic does not cause a
 * panic of any databases. And vice-versa a database panic does not cause a DBMS panic, with the single exception that a system db panic will cause a DBMS panic
 * because the system db is an essential part of the DBMS.
 */
public enum DatabasePanicReason implements Panicker.Reason
{
    TEST( "DatabasePanic just for testing" ),
    UNKNOWN_REASON( "See exception message for details" ),
    CATCHUP_FAILED( "Catchup failed" ),
    SNAPSHOT_FAILED( "Snapshot failed" ),
    COMMAND_APPLICATION_FAILED( "Command application failed" ),
    RAFT_MESSAGE_APPLIER_FAILED( "RaftMessage applier failed" );

    private final String description;

    DatabasePanicReason( String description )
    {
        this.description = description;
    }

    @Override
    public String getDescription()
    {
        return description;
    }
}
