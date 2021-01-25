/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stores;

import com.neo4j.causalclustering.common.Cluster;

import java.nio.file.Path;
import java.util.Optional;

public interface BackupStore
{
    Optional<DefaultDatabasesBackup> generate( Path backupDir, Cluster backupCluster ) throws Exception;
}
