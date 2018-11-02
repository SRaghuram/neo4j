/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup;

public class BackupResult
{
    private final boolean consistent;
    private final boolean transientErrorOnBackup;

    public BackupResult( boolean consistent, boolean transientErrorOnBackup )
    {
        this.consistent = consistent;
        this.transientErrorOnBackup = transientErrorOnBackup;
    }

    public boolean isConsistent()
    {
        return consistent;
    }

    public boolean isTransientErrorOnBackup()
    {
        return transientErrorOnBackup;
    }
}
