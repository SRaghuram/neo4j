/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

public interface Reporting
{
    void startedTransaction();

    void succeededTransaction( int serverId );

    void failedTransaction( int serverId, Throwable t );

    void fatalError( String message, Throwable t );

    void done();

    void clearErrors( int serverId );
}
