/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.restart;

import java.io.IOException;
import org.neo4j.kernel.impl.store.StoreType;

class State
{
    private final String name;
    private final StoreType[] completesStoreTypes;

    State( String name, StoreType... completesStoreTypes )
    {
        this.name = name;
        this.completesStoreTypes = completesStoreTypes;
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
    StoreType[] completesStoreTypes()
    {
        return completesStoreTypes;
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
     * @throws IOException on I/O error.
     */
    void run() throws IOException
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
