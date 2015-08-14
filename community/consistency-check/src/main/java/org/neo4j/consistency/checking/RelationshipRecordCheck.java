/**
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
package org.neo4j.consistency.checking;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.RecordField;
import org.neo4j.consistency.checking.full.FullCheckNewUtils;
import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.consistency.checking.full.StoreProcessorTask;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.DirectRecordReference;
import org.neo4j.consistency.store.FilteringRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public class RelationshipRecordCheck
        extends PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>
{
    public RelationshipRecordCheck()
    {
        super( true, RelationshipTypeField.RELATIONSHIP_TYPE, 
               NodeField.SOURCE, RelationshipField.SOURCE_PREV, RelationshipField.SOURCE_NEXT,
               NodeField.TARGET, RelationshipField.TARGET_PREV, RelationshipField.TARGET_NEXT);
    }

    @SafeVarargs
    private RelationshipRecordCheck( boolean firstProperty, RecordField<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>... fields )
    {
        super( firstProperty, fields );
    }
    public static RelationshipRecordCheck RelationshipRecordCheckPass1(boolean firstProperty)
    {
    	return  new RelationshipRecordCheck(firstProperty, RelationshipTypeField.RELATIONSHIP_TYPE);
    }
    public static RelationshipRecordCheck RelationshipRecordCheckPass2(boolean firstProperty)
    {
    	return new RelationshipRecordCheck(firstProperty, NodeField.SOURCE, NodeField.TARGET);
    }
    public static RelationshipRecordCheck RelationshipRecordCheckSourceChain(boolean firstProperty)
    {
    	RelationshipRecordCheck recordCheck = new RelationshipRecordCheck(firstProperty, RelationshipField.SOURCE_NEXT, RelationshipField.SOURCE_PREV, 
    			RelationshipField.TARGET_NEXT, RelationshipField.TARGET_PREV, RelationshipField.CACHE_VALUES);
    	recordCheck.setCacheRelationships(true);
    	return recordCheck;
    }
    public static RelationshipRecordCheck RelationshipRecordCheckTargetChain(boolean firstProperty)
    {
    	return  new RelationshipRecordCheck(firstProperty, RelationshipField.TARGET_NEXT, RelationshipField.TARGET_PREV);
    }
    

	public boolean isCacheRelationships() {
		return cacheRelationships;
	}

	public void setCacheRelationships(boolean cacheRelationships) {
		this.cacheRelationships = cacheRelationships;
	}

	private boolean cacheRelationships = false;

    private enum RelationshipTypeField implements
            RecordField<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>,
            ComparativeRecordChecker<RelationshipRecord, RelationshipTypeTokenRecord, ConsistencyReport.RelationshipConsistencyReport>
    {
        RELATIONSHIP_TYPE;

        @Override
        public void checkConsistency( RelationshipRecord record,
                                      CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                      RecordAccess records )
        {
            if ( record.getType() < 0 )
            {
                engine.report().illegalRelationshipType();
            }
            else
            {
                engine.comparativeCheck( records.relationshipType( record.getType() ), this );
            }
        }

        @Override
        public long valueFrom( RelationshipRecord record )
        {
            return record.getType();
        }

        @Override
        public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                                 CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                 DiffRecordAccess records )
        {
            // nothing to check
        }

        @Override
        public void checkReference( RelationshipRecord record, RelationshipTypeTokenRecord referred,
                                    CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                    RecordAccess records )
        {
            if ( !referred.inUse() )
            {
                engine.report().relationshipTypeNotInUse( referred );
            }
        }
    }

    private enum RelationshipField implements
            RecordField<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>,
            ComparativeRecordChecker<RelationshipRecord, RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>
    {
        SOURCE_PREV( NodeField.SOURCE )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getFirstPrevRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
            	return field.next(relationship);
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
            	if (report != null)
                report.sourcePrevReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
            	if (report != null)
                report.sourcePrevDoesNotReferenceBack( relationship );
            }

            @Override
            void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
            	if (report != null)
                report.sourcePrevNotUpdated();
            }

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.isFirst( record );
            }
            
            @Override
			RelationshipRecord setRelationship(RelationshipRecord rel) {
            	// this is for fP - so it has to be fN or sN
            	int threadIndex = FullCheckNewUtils.threadIndex.get();
            	if (cacheFields[threadIndex][0] == 0)
					rel.setFirstNextRel(cacheFields[threadIndex][3]);
				else
					rel.setSecondNextRel(cacheFields[threadIndex][3]);
				return rel;
            }
            @Override
			void linkChecked() {
				// TODO Auto-generated method stub
				FullCheckNewUtils.Counts.linksChecked[0][FullCheckNewUtils.threadIndex.get()]++;
			}
        },
        SOURCE_NEXT( NodeField.SOURCE )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getFirstNextRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
            	return field.prev(relationship);
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
            	if (report != null)
                report.sourceNextReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
            	if (report != null)
                report.sourceNextDoesNotReferenceBack( relationship );
            }

            @Override
            void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
            	if (report != null)
                report.sourceNextNotUpdated();
            }

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.next( record ) == Record.NO_NEXT_RELATIONSHIP.intValue();
            }

			@Override
			RelationshipRecord setRelationship(RelationshipRecord rel) {
				// this is for sN - so has to be fP or sP
				int threadIndex = FullCheckNewUtils.threadIndex.get();
				if (cacheFields[threadIndex][0] == 0)
					rel.setFirstPrevRel(cacheFields[threadIndex][3]);
				else
					rel.setSecondPrevRel(cacheFields[threadIndex][3]);				
				
				return rel;
			}
			@Override
			void linkChecked() {
				// TODO Auto-generated method stub
				FullCheckNewUtils.Counts.linksChecked[1][FullCheckNewUtils.threadIndex.get()]++;
			}
        },
        TARGET_PREV( NodeField.TARGET )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getSecondPrevRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
            	return field.next(relationship);
           
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
            	if (report != null)
                report.targetPrevReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
            	if (report != null)
                report.targetPrevDoesNotReferenceBack( relationship );
            }

            @Override
            void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
            	if (report != null)
                report.targetPrevNotUpdated();
            }

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.isFirst( record );
            }

			@Override
			RelationshipRecord setRelationship(RelationshipRecord rel) {
				// this for sP - so it has to be fN or sN
				int threadIndex = FullCheckNewUtils.threadIndex.get();
				if (cacheFields[threadIndex][0] == 0)
					rel.setFirstNextRel(cacheFields[threadIndex][3]);
				else
					rel.setSecondNextRel(cacheFields[threadIndex][3]);
				return rel;
			}
			@Override
			void linkChecked() {
				// TODO Auto-generated method stub
				FullCheckNewUtils.Counts.linksChecked[2][FullCheckNewUtils.threadIndex.get()]++;
			}
        },
        TARGET_NEXT( NodeField.TARGET )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getSecondNextRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
            	return field.prev(relationship);
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
            	if (report != null)
                report.targetNextReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
            	if (report != null)
                report.targetNextDoesNotReferenceBack( relationship );
            }

            @Override
            void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
            	if (report != null)
                report.targetNextNotUpdated();
            }

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.next( record ) == Record.NO_NEXT_RELATIONSHIP.intValue();
            }

			@Override
			RelationshipRecord setRelationship(RelationshipRecord rel) {
				// this for sN - it has to be either fP or sP depending on first or second node
				int threadIndex = FullCheckNewUtils.threadIndex.get();
				if (cacheFields[threadIndex][0] == 0)
					rel.setFirstPrevRel(cacheFields[threadIndex][3]);
				else
					rel.setSecondPrevRel(cacheFields[threadIndex][3]);
				return rel;
			}
			@Override
			void linkChecked() {
				// TODO Auto-generated method stub
				FullCheckNewUtils.Counts.linksChecked[3][FullCheckNewUtils.threadIndex.get()]++;
			}
			
        },
        CACHE_VALUES (null)
        {

			@Override
			public long valueFrom(RelationshipRecord record) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			boolean endOfChain(RelationshipRecord record) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			void notUpdated(RelationshipConsistencyReport report) {
				// TODO Auto-generated method stub
				
			}

			@Override
			long other(NodeField field, RelationshipRecord relationship) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			void otherNode(RelationshipConsistencyReport report, RelationshipRecord relationship) {
				// TODO Auto-generated method stub
				
			}

			@Override
			void noBackReference(RelationshipConsistencyReport report, RelationshipRecord relationship) {
				// TODO Auto-generated method stub
				
			}

			@Override
			RelationshipRecord setRelationship(RelationshipRecord rel) {
				// TODO Auto-generated method stub
				return null;
			}
			@Override
	        public void checkConsistency( RelationshipRecord relationship,
	                                      CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
	                                      RecordAccess records )
	        { 
				FullCheckNewUtils.saveToCache(relationship);
	        }

			@Override
			void linkChecked() {
				// TODO Auto-generated method stub
				FullCheckNewUtils.Counts.linksChecked[FullCheckNewUtils.threadIndex.get()][3]++;
			}
        	
        };

        protected final NodeField NODE;
        protected static long[][] cacheFields = new long[FullCheckNewUtils.MAX_THREADS][];

        private RelationshipField( NodeField node )
        {
            this.NODE = node;
        }

		private RecordReference<RelationshipRecord> buildFromCache(RelationshipRecord relationship, long id, long nodeId, 
				RecordAccess records) {
			int threadIndex = FullCheckNewUtils.threadIndex.get();
			if (!StoreProcessorTask.withinBounds(nodeId))
			{
				//wrong thread, so skip
				FullCheckNewUtils.Counts.correctSkipCheckCount[threadIndex]++;
				return ((FilteringRecordAccess)records).skipRel();
			}
			if (id != cacheFields[threadIndex][2] )
			{
				if (FullCheckNewUtils.isForward() && id > relationship.getId() ||
						!FullCheckNewUtils.isForward() && id < relationship.getId())
				{
					//wrong direction, so skip
					FullCheckNewUtils.Counts.correctSkipCheckCount[threadIndex]++;
					return ((FilteringRecordAccess)records).skipRel();
				}
				//these are "bad links", and hopefully few. So, get the real ones anyway.
				FullCheckNewUtils.Counts.missCheckCount[threadIndex]++;
				return records.relationship( id );				
			}

			// now, use cached info to build a fake relationship, but with partial real values that had been cached before
			RelationshipRecord rel = new RelationshipRecord(id);
			rel.setReal(false);		

			if (cacheFields[threadIndex][0] == 0)
				rel.setFirstNode(nodeId);
			else
				rel.setSecondNode(nodeId);
			
			rel = setRelationship(rel);

			return new DirectRecordReference<RelationshipRecord>( rel, records );
		}

        @Override
        public void checkConsistency( RelationshipRecord relationship,
                                      CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                      RecordAccess records )
        {
        	/* 
        	 * The algorithm for fast consistency check does 2 passes over the relationship store - one forward and one backward.
        	 * In both passes, typically the cached information is used build the referred relationship instead of going to disk.
        	 * This is what minimizes the random access to disk, but it is guaranteed that the cached information was always got
        	 * from disk at an appropriate opportunity and all links are correctly checked with right data.
        	 * In the forward pass, only the previous relationship information is cached and hence only the next information can 
        	 * be checked, while in backward pass only the next information is cached allowing checking of previous    
			*/
        	int threadIndex = FullCheckNewUtils.threadIndex.get();
            if ( !endOfChain( relationship ) )
            {
            	RecordReference<RelationshipRecord> referred = null;
            	long id = valueFrom( relationship );
            	long nodeId = -1;

        		if (((RecordAccess)records).shouldSkip( id, MultiPassStore.RELATIONSHIPS ))
        		{
        			if ((FullCheckNewUtils.isForward() && id > relationship.getId()) ||
        					(!FullCheckNewUtils.isForward() && id < relationship.getId()))
        			{
        				referred = ((FilteringRecordAccess)records).skipRel();
        			}
        		}
        		else
        		{
        			referred = ((FilteringRecordAccess)records).skipRel();
        			nodeId = NODE == NodeField.SOURCE ? relationship.getFirstNode() : relationship.getSecondNode();
        			
        			cacheFields[threadIndex] = FullCheckNewUtils.NewCCCache.getFromCache(nodeId);
        			
        			if (cacheFields[threadIndex][2] == 0x7ffffffffl) 
        			{
        				referred = ((FilteringRecordAccess)records).skipRel();
        				FullCheckNewUtils.Counts.noCacheSkipCount[threadIndex]++;
        			}
        			else
        			{
        				try {
        					referred = buildFromCache(relationship, id, nodeId, records );
        				} catch (Exception e)
        				{
        					System.out.println("error in buildFromCache");
        				}
        			if (referred.toString().equalsIgnoreCase("SkipReference"))            			 
        				FullCheckNewUtils.Counts.skipCheckCount[threadIndex]++;	
        			
        			}
        		} 
            	try {
            		engine.comparativeCheck( referred, this );
            	} catch (Exception e)
            	{
            		System.out.println("error from comparitiveCheck");
            	}
            	cacheFields[threadIndex] = null;
            	if (!referred.toString().equalsIgnoreCase("SkipReference"))
            	{
    				FullCheckNewUtils.Counts.checkedCount[threadIndex]++;
    				linkChecked();
            	}
            }
            else 
            {
            	FullCheckNewUtils.Counts.checkedCount[threadIndex]++;
            	linkChecked();
            }
        }

        @Override
        public void checkReference( RelationshipRecord record, RelationshipRecord referred,
                                    CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                    RecordAccess records )
        {
        	int threadIndex = FullCheckNewUtils.threadIndex.get();
            NodeField field = NodeField.select( referred, node( record ) ) ;
            if ( field == null )
            {
                otherNode( engine.report(), referred );
            }
            else
            {
                if ( other( field, referred ) != record.getId() )
                {
            		if (!referred.isReal() )//&& isSkippable(record, cacheFields))
            		{
            			//get the actual record and check again
            			RecordReference<RelationshipRecord> refRel =
            					records.relationship( referred.getId() );
            			referred = (RelationshipRecord)((DirectRecordReference)refRel).record();
            			checkReference(record, referred, engine, records);
            			FullCheckNewUtils.Counts.skipBackupCount[threadIndex]++;
            		}
            		else
            		{
            			FullCheckNewUtils.Counts.checkErrors[threadIndex]++;
            			noBackReference( engine == null ? null : engine.report(), referred );
            		}
                } 
                else
                { 	// successfully checked
                	// clear cache only if cache is used - meaning referred was built using cache.
                	if (!referred.isReal()) 
                	{               		
            				FullCheckNewUtils.initCache(node( record ));
                	}
                	
                }
            }
        }

        @Override
        public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                                 CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                 DiffRecordAccess records )
        {
            if ( !newRecord.inUse() || valueFrom( oldRecord ) != valueFrom( newRecord ) )
            {   // if we're deleting or creating this relationship record
                if ( !endOfChain( oldRecord )
                     && records.changedRelationship( valueFrom( oldRecord ) ) == null )
                {   // and we didn't update an expected pointer --> report
                    notUpdated( engine.report() );
                }
            }
        }

        abstract boolean endOfChain( RelationshipRecord record );

        abstract void notUpdated( ConsistencyReport.RelationshipConsistencyReport report );

        abstract long other( NodeField field, RelationshipRecord relationship );

        abstract void otherNode( ConsistencyReport.RelationshipConsistencyReport report,
                                 RelationshipRecord relationship );

        abstract void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                       RelationshipRecord relationship );

        abstract RelationshipRecord setRelationship(RelationshipRecord rel);
        abstract void linkChecked();
        private long node( RelationshipRecord relationship )
        {
            return NODE.valueFrom( relationship );
        }
    }
}
