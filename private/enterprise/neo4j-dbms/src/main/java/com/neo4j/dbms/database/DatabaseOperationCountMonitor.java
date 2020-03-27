/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

public interface DatabaseOperationCountMonitor
{
    long createCount();

    long startCount();

    long stopCount();

    long dropCount();

    long failedCount();

    long recoveredCount();

    void increaseCreateCount();

    void increaseStartCount();

    void increaseStopCount();

    void increaseDropCount();

    void increaseFailedCount();

    void increaseRecoveredCount();

    void resetCounts();
}
