/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.tools;

import org.neo4j.io.layout.DatabaseLayout;

public interface BackupConsistencyChecker
{
    BackupConsistencyChecker NO_OP = databaseLayout ->
    {
    };

    void checkConsistency( DatabaseLayout databaseLayout ) throws ConsistencyCheckExecutionException;
}
