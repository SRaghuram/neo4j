/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bookmark;

import com.neo4j.fabric.bolt.FabricBookmark;
import com.neo4j.fabric.driver.RemoteBookmark;
import com.neo4j.fabric.executor.Location;

import java.util.List;

import org.neo4j.bolt.runtime.Bookmark;

/**
 * Manges bookmarks for a single transaction.
 * <p>
 * An implementation MUST NOT be shared across transactions.
 */
public interface TransactionBookmarkManager
{
    void processSubmittedByClient( List<Bookmark> bookmarks );

    /**
     * Returns bookmarks that should be sent to a remote when opening a transaction there.
     */
    List<RemoteBookmark> getBookmarksForRemote( Location.Remote location );

    /**
     * Handle a bookmark received from a remote after a transaction has been committed there.
     */
    void remoteTransactionCommitted( Location.Remote location, RemoteBookmark bookmark );

    /**
     * Will wait until the local graph in a state required by bookmarks submitted in {@link #processSubmittedByClient(List)}.
     */
    void awaitUpToDate( Location.Local location );

    /**
     * Notifies the manager that a local transaction has been committed.
     */
    void localTransactionCommitted( Location.Local local );

    /**
     * Constructs a bookmark that will hold the information collected by this bookmark manager.
     */
    FabricBookmark constructFinalBookmark();
}
