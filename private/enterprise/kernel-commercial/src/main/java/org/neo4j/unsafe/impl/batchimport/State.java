/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.unsafe.impl.batchimport;

import java.io.IOException;
import org.neo4j.kernel.impl.store.StoreType;

class State
{
    interface CheckPointer
    {
        void checkPoint( byte[] checkPoint ) throws IOException;
    }

    private final String name;
    private final StoreType[] completesMainStoreTypes;
    private final StoreType[] completesTempStoreTypes;

    State( String name, StoreType[] completesMainStoreTypes, StoreType[] completesTempStoreTypes )
    {
        this.name = name;
        this.completesMainStoreTypes = completesMainStoreTypes;
        this.completesTempStoreTypes = completesTempStoreTypes;
    }

    /**
     * @return name of this state, also persisted.
     */
    String name()
    {
        return name;
    }

    /**
     * @return which stores this state completes, i.e. to keep.
     */
    StoreType[] completesMainStoreTypes()
    {
        return completesMainStoreTypes;
    }

    /**
     * @return which stores this state completes, i.e. to keep.
     */
    StoreType[] completesTempStoreTypes()
    {
        return completesTempStoreTypes;
    }

    /**
     * Load additional data required when loading directly into this state. Typically loads one or more, although not necessarily all,
     * things, {@link #save() saved} by previous states.
     *
     * @throws IOException on I/O error.
     */
    void load() throws IOException
    {   // left empty here, optionally implemented by subclass
    }

    /**
     * Runs the logic of this state. This assumes that everything this state needs is good and ready to go.
     *
     * @param fromCheckPoint last completed check point from previous run.
     * @param checkPointer notified about checkpoints while running this state.
     * @throws IOException on I/O error.
     */
    void run( byte[] fromCheckPoint, CheckPointer checkPointer ) throws IOException
    {   // left empty here, optionally implemented by subclass
    }

    /**
     * Saves additional data so that it may be loaded, if required, for later {@link #load() loading} some state after this one.
     *
     * @throws IOException on I/O error.
     */
    void save() throws IOException
    {   // left empty here, optionally implemented by subclass
    }
}
