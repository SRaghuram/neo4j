/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.consistency.store.StoreAccess;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static org.neo4j.kernel.impl.store.RecordStore.IN_USE;
import static org.neo4j.consistency.checking.full.StoreProcessorTask.Scanner.scan;

public class IterableStore<RECORD extends AbstractBaseRecord> implements BoundedIterable<RECORD>
{
    private final RecordStore<RECORD> store;
    private boolean forward = true;
    private StoreAccess storeAccess;

    public IterableStore( RecordStore<RECORD> store, StoreAccess storeAccess )
    {
        this.store = store;
        this.storeAccess = storeAccess;
    }

    @Override
    public long maxCount()
    {
        return store.getHighId();
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public Iterator<RECORD> iterator()
    {
        return scan( store, storeAccess, forward, IN_USE ).iterator();
    }
    
    public void setDirection(boolean forward )
    {
    	this.forward = forward;
    }
    
    public void warmUpCache()
    {
        int recordsPerPage =  ((CommonAbstractStore)store).getPageSize()/store.getRecordSize();
        long id = 0;
        while (id < store.getHighId()/2)
        {
            RECORD record = store.getRecord( id );
            id += recordsPerPage;
        }
    }
}
