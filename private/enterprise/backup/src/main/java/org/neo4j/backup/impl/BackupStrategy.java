/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.lifecycle.Lifecycle;

interface BackupStrategy extends Lifecycle
{
    void performIncrementalBackup( DatabaseLayout targetDbLayout, AdvertisedSocketAddress address ) throws BackupExecutionException;

    void performFullBackup( DatabaseLayout targetDbLayout, AdvertisedSocketAddress address ) throws BackupExecutionException;
}
