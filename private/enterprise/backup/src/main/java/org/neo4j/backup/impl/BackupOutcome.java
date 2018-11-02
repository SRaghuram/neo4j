/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

public class BackupOutcome
{
    private final boolean consistent;
    private final long lastCommittedTx;

    BackupOutcome( long lastCommittedTx, boolean consistent )
    {
        this.lastCommittedTx = lastCommittedTx;
        this.consistent = consistent;
    }

    public long getLastCommittedTx()
    {
        return lastCommittedTx;
    }

    public boolean isConsistent()
    {
        return consistent;
    }
}
