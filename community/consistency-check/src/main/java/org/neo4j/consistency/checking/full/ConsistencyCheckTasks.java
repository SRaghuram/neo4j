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

import org.neo4j.consistency.checking.NodeRecordCheck;
import org.neo4j.consistency.checking.RelationshipRecordCheck;
import org.neo4j.consistency.checking.SchemaRecordCheck;
import org.neo4j.consistency.checking.full.IndexCheck;
import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.consistency.checking.full.StoppableRunnable;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.checking.index.IndexEntryProcessor;
import org.neo4j.consistency.checking.index.IndexIterator;
import org.neo4j.consistency.checking.labelscan.LabelScanCheck;
import org.neo4j.consistency.checking.labelscan.LabelScanDocumentProcessor;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.store.StoreAccess;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static java.lang.String.format;
import static org.neo4j.consistency.checking.full.MultiPassStore.ARRAYS;
import static org.neo4j.consistency.checking.full.MultiPassStore.LABELS;
import static org.neo4j.consistency.checking.full.MultiPassStore.NODES;
import static org.neo4j.consistency.checking.full.MultiPassStore.PROPERTIES;
import static org.neo4j.consistency.checking.full.MultiPassStore.RELATIONSHIPS;
import static org.neo4j.consistency.checking.full.MultiPassStore.RELATIONSHIP_GROUPS;
import static org.neo4j.consistency.checking.full.MultiPassStore.STRINGS;

public class ConsistencyCheckTasks
{
    private final ProgressMonitorFactory.MultiPartBuilder progress;
    private final TaskExecutionOrder order;
    private final StoreProcessor processor;
    private StoreAccess nativeStores = null;
    private MultiPassStore.FactoryNew multiPass;
    private ConsistencyReporter reporter;
    private LabelScanStore labelScanStore;
    private IndexAccessors indexes;

    ConsistencyCheckTasks( ProgressMonitorFactory.MultiPartBuilder progress, TaskExecutionOrder order,
            StoreProcessor processor, StoreAccess nativeStores, LabelScanStore labelScanStore,
            IndexAccessors indexes, MultiPassStore.FactoryNew multiPass, ConsistencyReporter reporter )
    {
        this.progress = progress;
        this.order = order;
        this.processor = processor;
        this.nativeStores = nativeStores;
        this.multiPass = multiPass;
        this.reporter = reporter;
        this.labelScanStore = labelScanStore;
        this.indexes = indexes;
    }

    public List<StoppableRunnable> createTasksForFullCheck( boolean checkLabelScanStore, boolean checkIndexes,
            boolean checkGraph )
    {
        List<StoppableRunnable> tasks = new ArrayList<>();
        if ( checkGraph )
        {
            StoreProcessor processor =
                    multiPass.processor( FullCheckNewUtils.Stages.Stage1_NS_PropsLabels, PROPERTIES, 1, 63 );
            multiPass.reDecorateNode( processor, null, true );
            tasks.add( create( FullCheckNewUtils.Stages.Stage1_NS_PropsLabels.name(), nativeStores.getNodeStore(),
                    processor ) );
            //ReltionshipStore pass - check label counts using cached labels, check properties, skip nodes and relationships
            processor = multiPass.processor( FullCheckNewUtils.Stages.Stage2_RS_Labels, LABELS );
            multiPass.reDecorateRelationship( processor, RelationshipRecordCheck.RelationshipRecordCheckPass1( false ) );
            tasks.add( create( FullCheckNewUtils.Stages.Stage2_RS_Labels.name(), nativeStores.getRelationshipStore(),
                    processor ) );
            //NodeStore pass - just cache nextRel and inUse
            tasks.add( create( FullCheckNewUtils.Stages.Stage3_NS_NextRel.name(), nativeStores.getNodeStore(),
                    new StoreProcessor[] {new CacheProcessor( FullCheckNewUtils.Stages.Stage3_NS_NextRel,
                            CacheProcessor.CacheAction.GET_NEXTREL, nativeStores, 1, 1, 35 )} ) );
            //RelationshipStore pass - check nodes inUse, FirstInFirst, FirstInSecond using cached info
            processor = multiPass.processor( FullCheckNewUtils.Stages.Stage4_RS_NextRel, NODES );
            multiPass.reDecorateRelationship( processor, RelationshipRecordCheck.RelationshipRecordCheckPass2( true ) );
            tasks.add( create( FullCheckNewUtils.Stages.Stage4_RS_NextRel.name(), nativeStores.getRelationshipStore(),
                    processor ) );
            //NodeStore pass - just cache nextRel and inUse
            multiPass.reDecorateNode( processor, NodeRecordCheck.toCheckNextRel( false ), true );
            multiPass.reDecorateNode( processor, NodeRecordCheck.toCheckNextRelationshipGroup( false ), false );
            tasks.add( create( FullCheckNewUtils.Stages.Stage5_Check_NextRel.name(), nativeStores.getNodeStore(),
                    new CacheProcessor( FullCheckNewUtils.Stages.Stage5_Check_NextRel,
                            CacheProcessor.CacheAction.CHECK_NEXTREL, nativeStores, processor ) ) );
            // source chain
            //RelationshipStore pass - forward scan of source chain using the cache.
            processor = multiPass.processor( FullCheckNewUtils.Stages.Stage6_RS_Forward, RELATIONSHIPS, 1, 1, 35, 35 );
            multiPass.reDecorateRelationship( processor,
                    RelationshipRecordCheck.RelationshipRecordCheckSourceChain( false ) );
            tasks.add( create( FullCheckNewUtils.Stages.Stage6_RS_Forward.name(), nativeStores.getRelationshipStore(),
                    processor ) );
            //RelationshipStore pass - reverse scan of source chain using the cache.            
            processor = multiPass.processor( FullCheckNewUtils.Stages.Stage7_RS_Backward, RELATIONSHIPS );
            processor.reverseDirection();
            multiPass.reDecorateRelationship( processor,
                    RelationshipRecordCheck.RelationshipRecordCheckSourceChain( false ) );
            tasks.add( create( FullCheckNewUtils.Stages.Stage7_RS_Backward.name(), nativeStores.getRelationshipStore(),
                    processor ) );
            //tasks.add( create( FullCheckNewUtils.Stages.Stage8_PS_Props.name(), nativeStores.getPropertyStore(),
            //        multiPass.processor(FullCheckNewUtils.Stages.Stage8_PS_Props, PROPERTIES)));//, STRINGS, ARRAYS, PROPERTY_KEYS ) ) ); 
            tasks.add( new RecordScanner<>( new IterableStore<>( nativeStores.getNodeStore(), nativeStores ),
                    FullCheckNewUtils.Stages.Stage8_PS_Props.name(), progress,
                    new PropertyAndNode2LabelIndexProcessor( reporter, (checkIndexes ? indexes : null), new PropertyReader( nativeStores ) ),
                    FullCheckNewUtils.Stages.Stage8_PS_Props ) );
            
            tasks.add( create( "StrStore-Str", nativeStores.getStringStore(), multiPass.processors( STRINGS ) ) );
            tasks.add( create( "ArrayStore-Arrays", nativeStores.getArrayStore(), multiPass.processors( ARRAYS ) ) );
            tasks.add( create( "RelGrpSt-RelGrp", nativeStores.getRelationshipGroupStore(),
                    multiPass.processors( RELATIONSHIP_GROUPS ) ) );
        }
        // The schema store is verified in multiple passes that share state since it fits into memory
        // and we care about the consistency of back references (cf. SemanticCheck)
        // PASS 1: Dynamic record chains
        tasks.add( create( "SchemaStore", nativeStores.getSchemaStore() ) );
        // PASS 2: Rule integrity and obligation build up
        final SchemaRecordCheck schemaCheck =
                new SchemaRecordCheck( new SchemaStorage( nativeStores.getSchemaStore() ) );
        tasks.add( new SchemaStoreProcessorTask<>( nativeStores.getSchemaStore(), "check_rules", schemaCheck, progress,
                order, processor, processor ) );
        // PASS 3: Obligation verification and semantic rule uniqueness
        tasks.add( new SchemaStoreProcessorTask<>( nativeStores.getSchemaStore(), "check_obligations", schemaCheck
                .forObligationChecking(), progress, order, processor, processor ) );
        if ( checkGraph )
        {
            tasks.add( create( "RelationshipTypeTokenStore", nativeStores.getRelationshipTypeTokenStore() ) );
            tasks.add( create( "PropertyKeyTokenStore", nativeStores.getPropertyKeyTokenStore() ) );
            tasks.add( create( "LabelTokenStore", nativeStores.getLabelTokenStore() ) );
            tasks.add( create( "RelationshipTypeNameStore", nativeStores.getRelationshipTypeNameStore() ) );
            tasks.add( create( "PropertyKeyNameStore", nativeStores.getPropertyKeyNameStore() ) );
            tasks.add( create( "LabelNameStore", nativeStores.getLabelNameStore() ) );
            tasks.add( create( "NodeDynamicLabelStore", nativeStores.getNodeDynamicLabelStore() ) );
        }
        if ( checkLabelScanStore )
        {
            tasks.add( new RecordScanner<>( new IterableStore<>( nativeStores.getNodeStore(), nativeStores ),
                    "NodeStoreToLabelScanStore", progress,
                    new NodeToLabelScanRecordProcessor( reporter, labelScanStore ),
                    FullCheckNewUtils.Stages.Stage9_NS_LabelCounts ) );
        }
        int iPass = 0;
        for ( ConsistencyReporter filteredReporter : multiPass.reporters( order, NODES ) )
        {
            if ( checkLabelScanStore )
            {
                tasks.add( new RecordScanner<>( labelScanStore.newAllEntriesReader(), format( "LabelScanStore_%d",
                        iPass ), progress, new LabelScanDocumentProcessor( filteredReporter, new LabelScanCheck() ) ) );
            }
            if ( checkIndexes )
            {
                for ( IndexRule indexRule : indexes.rules() )
                {
                    tasks.add( new RecordScanner<>( new IndexIterator( indexes.accessorFor( indexRule ) ), format(
                            "Index_%d_%d", indexRule.getId(), iPass ), progress, new IndexEntryProcessor(
                            filteredReporter, new IndexCheck( indexRule ) ) ) );
                }
            }
            iPass++;
        }
        return tasks;
    }

    private <RECORD extends AbstractBaseRecord> StoreProcessorTask<RECORD> create( String name,
            RecordStore<RECORD> input )
    {
        return new StoreProcessorTask<>( name, input, progress, order, processor, processor );
    }

    private <RECORD extends AbstractBaseRecord> StoreProcessorTask<RECORD> create( String name,
            RecordStore<RECORD> input, StoreProcessor[] processors )
    {
        StoreProcessorTask<RECORD> storeProcessorTask =
                new StoreProcessorTask<RECORD>( name, input, progress, order, processors[0], processors );
        storeProcessorTask.setStoreAccess( nativeStores );
        return storeProcessorTask;
    }

    private <RECORD extends AbstractBaseRecord> StoreProcessorTask<RECORD> create( String name,
            RecordStore<RECORD> input, StoreProcessor processor )
    {
        StoreProcessorTask<RECORD> storeProcessorTask =
                new StoreProcessorTask<RECORD>( name, input, progress, order, processor,
                        new StoreProcessor[] {processor} );
        storeProcessorTask.setStoreAccess( nativeStores );
        return storeProcessorTask;
    }
}
