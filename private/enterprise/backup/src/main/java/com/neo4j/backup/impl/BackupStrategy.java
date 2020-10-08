/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.v4.metadata.IncludeMetadata;

import java.util.Optional;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.lifecycle.Lifecycle;

interface BackupStrategy extends Lifecycle
{
    void performIncrementalBackup( DatabaseLayout targetDbLayout, SocketAddress address, String databaseName,
                                   Optional<IncludeMetadata> includeMetadata ) throws BackupExecutionException;

    void performFullBackup( DatabaseLayout targetDbLayout, SocketAddress address, String databaseName,
                            Optional<IncludeMetadata> includeMetadata ) throws BackupExecutionException;
}
