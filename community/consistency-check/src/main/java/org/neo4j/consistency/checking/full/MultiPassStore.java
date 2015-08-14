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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.NodeRecordCheck;
import org.neo4j.consistency.checking.RelationshipRecordCheck;
import org.neo4j.consistency.checking.full.FullCheckNewUtils;
import org.neo4j.consistency.checking.full.StoreProcessor;
import org.neo4j.consistency.checking.full.TaskExecutionOrder;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.FilteringRecordAccess;
import org.neo4j.consistency.store.StoreAccess;
import org.neo4j.kernel.impl.store.RecordStore;

public enum MultiPassStore
{
    NODES
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getNodeStore();
                }
            },
    RELATIONSHIPS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getRelationshipStore();
                }
            },
    PROPERTIES
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getPropertyStore();
                }
            },
    PROPERTY_KEYS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getPropertyKeyTokenStore();
                }
            },
    STRINGS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getNodeStore();
                }
            },
    ARRAYS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getNodeStore();
                }
            },
    LABELS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getLabelTokenStore();
                }
            },
    RELATIONSHIP_GROUPS
            {
                @Override
                RecordStore<?> getRecordStore( StoreAccess storeAccess )
                {
                    return storeAccess.getRelationshipGroupStore();
                }
            };

    public static boolean recordInCurrentPass( long id, int iPass, long recordsPerPass )
    {
        return id >= iPass * recordsPerPass && id < (iPass + 1) * recordsPerPass;
    }

    public List<DiffRecordAccess> multiPassFilters( long memoryPerPass, StoreAccess storeAccess,
            DiffRecordAccess recordAccess, MultiPassStore[] stores )
    {
        List<DiffRecordAccess> filteringStores = new ArrayList<>();
        RecordStore<?> recordStore = getRecordStore( storeAccess );
        long recordsPerPass = memoryPerPass / recordStore.getRecordSize();
        long highId = recordStore.getHighId();
        for ( int iPass = 0; iPass * recordsPerPass <= highId; iPass++ )
        {
            filteringStores.add( new FilteringRecordAccess( recordAccess, storeAccess, this, stores ) );
        }
        return filteringStores;
    }


    abstract RecordStore<?> getRecordStore( StoreAccess storeAccess );

    static class Factory
    {
        private final CheckDecorator decorator;
        private final DiffRecordAccess recordAccess;
        private final long totalMappedMemory;
        private final StoreAccess storeAccess;
        private final InconsistencyReport report;

        Factory( CheckDecorator decorator, long totalMappedMemory,
                 StoreAccess storeAccess, DiffRecordAccess recordAccess, InconsistencyReport report )
        {
            this.decorator = decorator;
            this.totalMappedMemory = totalMappedMemory;
            this.storeAccess = storeAccess;
            this.recordAccess = recordAccess;
            this.report = report;
        }

        ConsistencyReporter[] reporters( TaskExecutionOrder order, MultiPassStore... stores )
        {
            if ( order == TaskExecutionOrder.MULTI_PASS )
            {
                return reporters( stores );
            }
            else
            {
                return new ConsistencyReporter[]{new ConsistencyReporter( recordAccess, report )};
            }
        }

        ConsistencyReporter[] reporters( MultiPassStore... stores )
        {
            List<ConsistencyReporter> result = new ArrayList<>();
            for ( MultiPassStore store : stores )
            {
                List<DiffRecordAccess> filters = store.multiPassFilters( totalMappedMemory, storeAccess,
                        recordAccess, stores );
                for ( DiffRecordAccess filter : filters )
                {
                    result.add( new ConsistencyReporter( filter, report ) );
                }
            }
            return result.toArray( new ConsistencyReporter[result.size()] );
        }

        StoreProcessor[] processors( MultiPassStore... stores )
        {
            List<StoreProcessor> result = new ArrayList<>();
            for ( ConsistencyReporter reporter : reporters( stores ) )
            {
                result.add( new StoreProcessor( decorator, reporter ) );
            }
            return result.toArray( new StoreProcessor[result.size()] );
        }
    }
    
    //---------------New

    public List<DiffRecordAccess> multiPassFiltersNew( StoreAccess storeAccess,
            DiffRecordAccess recordAccess, MultiPassStore[] stores )
    {
        List<DiffRecordAccess> filteringStores = new ArrayList<>();
        RecordStore<?> recordStore = getRecordStore( storeAccess );
        long highId = recordStore.getHighId();
        
        filteringStores.add( new FilteringRecordAccess( recordAccess, storeAccess, this, stores ) );
        return filteringStores;
    }

    public DiffRecordAccess multiPassFilterNew( StoreAccess storeAccess,
            DiffRecordAccess recordAccess, MultiPassStore... stores )
    {
        RecordStore<?> recordStore = getRecordStore( storeAccess );
        long highId = recordStore.getHighId();    
        return new FilteringRecordAccess( recordAccess, storeAccess, this, stores );
    }

    static public class FactoryNew
    {
        private final CheckDecorator decorator;
        private final DiffRecordAccess recordAccess;
        private final StoreAccess storeAccess;
        private final InconsistencyReport report;

        public FactoryNew( CheckDecorator decorator, 
                 StoreAccess storeAccess, DiffRecordAccess recordAccess, InconsistencyReport report )
        {
            this.decorator = decorator;
            this.storeAccess = storeAccess;
            this.recordAccess = recordAccess;
            this.report = report;
        }

        public ConsistencyReporter[] reporters( TaskExecutionOrder order, MultiPassStore... stores )
        {
            if ( order == TaskExecutionOrder.MULTI_PASS )
            {
                return reporters( stores );
            }
            else
            {
                return new ConsistencyReporter[]{new ConsistencyReporter( recordAccess, report )};
            }
        }

        public ConsistencyReporter[] reporters( MultiPassStore... stores )
        {
            List<ConsistencyReporter> result = new ArrayList<>();
            for ( MultiPassStore store : stores )
            {
                List<DiffRecordAccess> filters = store.multiPassFiltersNew( storeAccess,
                        recordAccess, stores );
                for ( DiffRecordAccess filter : filters )
                {
                    result.add( new ConsistencyReporter( filter, report ) );
                }
            }
            return result.toArray( new ConsistencyReporter[result.size()] );
        }

        public StoreProcessor[] processors( MultiPassStore... stores )
        {
            List<StoreProcessor> result = new ArrayList<>();
            for ( ConsistencyReporter reporter : reporters( stores ) )
            {
                result.add( new StoreProcessor( decorator, reporter ) );
            }
            return result.toArray( new StoreProcessor[result.size()] );
        }
        public ConsistencyReporter reporter( MultiPassStore store )
        {
                DiffRecordAccess filter = store.multiPassFilterNew( storeAccess,
                        recordAccess, store );
                 return new ConsistencyReporter( filter, report ) ;
        }
        public StoreProcessor processor( FullCheckNewUtils.Stages stage, MultiPassStore store, int... cacheFields )
        {
            StoreProcessor processor = new StoreProcessor( decorator, reporter(store), stage);
            processor.setCacheFields(cacheFields);
            return processor;
        }
        
        public void reDecorateNode(StoreProcessor processer, NodeRecordCheck newDecorator, boolean sparseNode )
        {
            processer.reDecorateNode(decorator, newDecorator, sparseNode);
        }
        public void reDecorateRelationship(StoreProcessor processer, RelationshipRecordCheck newDecorator)
        {
            processer.reDecorateRelationship(decorator, newDecorator );
        }
    }
}
