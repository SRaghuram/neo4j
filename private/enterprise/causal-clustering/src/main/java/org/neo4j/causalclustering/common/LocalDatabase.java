/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.common;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Instances implementing this interface represent individual clustered databases in Neo4j.
 *
 * Instances are responsible for the lifecycle management of per-database components, as well as exposing
 * per database dependency management, monitoring and io operations.
 *
 * Collections of these instances should be managed by a {@link DatabaseService}
 */
public interface LocalDatabase extends Lifecycle
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
     * Returns a per-database {@link Dependencies} object.
     * These per-database dependencies sit in a tree underneath the parent, global dependencies.
     * If you `satisfy` an instance of a type on this object then you may only `resolve` it on this object.
     * However, if you `resolve` a type which is satisfied on the global dependencies but not here, that
     * will work fine. You will receive the global instance.
     *
     * @return dependencies service for this database
     */
    Dependencies dependencies();

    /**
     * Delete the store files for this database
     * @throws IOException
     */
    void delete() throws IOException;

    /**
     * @return Whether or not the store files for this database are empty/non-existent.
     * @throws IOException
     */
    boolean isEmpty() throws IOException;

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
     * Returns the {@link Database} which actually underpins this datasource. Exposes all sorts of kernel level machinery.
     * @return the underlying datasource for this database
     */
    Database database();

    /**
     * @return the name of this database
     */
    String databaseName();
}
