/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport.backup_stores;

import java.io.File;
import java.util.Optional;

import com.neo4j.causalclustering.common.Cluster;

public class NoStore implements BackupStore
{
    @Override
    public Optional<File> generate( File backupDir, Cluster<?> backupCluster )
    {
        return Optional.empty();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
