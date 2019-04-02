/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;

import java.io.File;
import java.io.IOException;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

/**
 * Instances implementing this interface represent individual clustered databases in Neo4j.
 *
 * Instances are responsible for the lifecycle management of per-database components, as well as exposing
 * per database dependency management, monitoring and io operations.
 *
 * TODO: Stop ClusteredDatabaseContext from being lifecycle and just manage the parts which need it in the database manager. Probably requires
 *  a Core and RR DatabaseManager, which is frustrating, but most of the implementation will be in the abstract version
 *
 * Collections of these instances should be managed by a {@link ClusteredDatabaseManager}
 */
public interface ClusteredDatabaseContext extends Lifecycle, DatabaseContext
{
    /**
     * Reads metadata about this database from disk and calculates a uniquely {@link StoreId}.
     * The store id should be cached so that future calls do not require IO.
     * @return store id for this database
     */
    StoreId storeId();

    /**
     * Returns per-database {@link Monitors}
     * @return monitors for this database
     */
    Monitors monitors();

    /**
     * Delete the store files for this database
     * @throws IOException
     */
    void delete() throws IOException;

    /**
     * @return Whether or not the store files for this database are empty/non-existent.
     */
    boolean isEmpty();

    /**
     * @return A listing of all store files which comprise this database
     */
    DatabaseLayout databaseLayout();

    /**
     * Replace the store files for this database
     * @param sourceDir the store files to replace this databases's current files with
     * @throws IOException
     */
    void replaceWith( File sourceDir ) throws IOException;

    /**
     * @return the name of this database
     */
    String databaseName();

    /**
     * @return the {@link CatchupComponentsRepository.DatabaseCatchupComponents} for this clustered database
     */
    CatchupComponentsRepository.DatabaseCatchupComponents catchupComponents();
}
