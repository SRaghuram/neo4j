/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.backup_stores;

import java.io.File;
import java.util.Optional;

import org.neo4j.causalclustering.common.Cluster;

public interface BackupStore
{
    Optional<File> generate( File backupDir, Cluster<?> backupCluster ) throws Exception;
}
