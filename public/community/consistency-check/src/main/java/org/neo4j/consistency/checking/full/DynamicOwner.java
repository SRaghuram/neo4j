/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.PendingReferenceCheck;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;

import static org.neo4j.consistency.store.RecordReference.SkippingReference.skipReference;

abstract class DynamicOwner<RECORD extends AbstractBaseRecord> implements Owner
{
    static final ComparativeRecordChecker<DynamicRecord, AbstractBaseRecord, ConsistencyReport.DynamicConsistencyReport>
            ORPHAN_CHECK =
            ( record, ignored, engine, records, cursorTracer ) -> engine.report().orphanDynamicRecord();

    abstract RecordReference<RECORD> record( RecordAccess records, PageCursorTracer cursorTracer );

    @Override
    public void checkOrphanage()
    {
        // default: do nothing
    }

    static class Property extends DynamicOwner<PropertyRecord>
            implements ComparativeRecordChecker<PropertyRecord, AbstractBaseRecord, ConsistencyReport.PropertyConsistencyReport>
    {
        private final long id;
        private final RecordType type;

        Property( RecordType type, PropertyRecord record )
        {
            this.type = type;
            this.id = record.getId();
        }

        @Override
        RecordReference<PropertyRecord> record( RecordAccess records, PageCursorTracer cursorTracer )
        {
            return records.property( id, cursorTracer );
        }

        @Override
        public void checkReference( PropertyRecord property, AbstractBaseRecord record,
                                    CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                    RecordAccess records, PageCursorTracer cursorTracer )
        {
            if ( record instanceof PropertyRecord )
            {
                if ( type == RecordType.STRING_PROPERTY )
                {
                    engine.report().stringMultipleOwners( (PropertyRecord) record );
                }
                else
                {
                    engine.report().arrayMultipleOwners( (PropertyRecord) record );
                }
            }
            else if ( record instanceof DynamicRecord )
            {
                if ( type == RecordType.STRING_PROPERTY )
                {
                    engine.report().stringMultipleOwners( (DynamicRecord) record );
                }
                else
                {
                    engine.report().arrayMultipleOwners( (DynamicRecord) record );
                }
            }
        }
    }

    static class Dynamic extends DynamicOwner<DynamicRecord>
            implements ComparativeRecordChecker<DynamicRecord, AbstractBaseRecord, ConsistencyReport.DynamicConsistencyReport>
    {
        private final long id;
        private final RecordType type;

        Dynamic( RecordType type, DynamicRecord record )
        {
            this.type = type;
            this.id = record.getId();
        }

        @Override
        RecordReference<DynamicRecord> record( RecordAccess records, PageCursorTracer cursorTracer )
        {
            switch ( type )
            {
            case STRING_PROPERTY:
                return records.string( id, cursorTracer );
            case ARRAY_PROPERTY:
                return records.array( id, cursorTracer );
            case PROPERTY_KEY_NAME:
                return records.propertyKeyName( (int)id, cursorTracer );
            case RELATIONSHIP_TYPE_NAME:
                return records.relationshipTypeName( (int) id, cursorTracer );
            default:
                return skipReference();
            }
        }

        @Override
        public void checkReference( DynamicRecord block, AbstractBaseRecord record,
                CheckerEngine<DynamicRecord,ConsistencyReport.DynamicConsistencyReport> engine, RecordAccess records, PageCursorTracer cursorTracer )
        {
            if ( record instanceof PropertyRecord )
            {
                engine.report().nextMultipleOwners( (PropertyRecord) record );
            }
            else if ( record instanceof DynamicRecord )
            {
                engine.report().nextMultipleOwners( (DynamicRecord) record );
            }
            else if ( record instanceof RelationshipTypeTokenRecord )
            {
                engine.report().nextMultipleOwners( (RelationshipTypeTokenRecord) record );
            }
            else if ( record instanceof PropertyKeyTokenRecord )
            {
                engine.report().nextMultipleOwners( (PropertyKeyTokenRecord) record );
            }
        }
    }

    abstract static class NameOwner<RECORD extends TokenRecord,
            REPORT extends ConsistencyReport.NameConsistencyReport> extends DynamicOwner<RECORD>
            implements ComparativeRecordChecker<RECORD, AbstractBaseRecord, REPORT>
    {
        @SuppressWarnings( "ConstantConditions" )
        @Override
        public void checkReference( RECORD name, AbstractBaseRecord record, CheckerEngine<RECORD,REPORT> engine, RecordAccess records,
                PageCursorTracer cursorTracer )
        {
            ConsistencyReport.NameConsistencyReport report = engine.report();
            if ( record instanceof RelationshipTypeTokenRecord )
            {
                ((ConsistencyReport.RelationshipTypeConsistencyReport) report)
                        .nameMultipleOwners( (RelationshipTypeTokenRecord) record );
            }
            else if ( record instanceof PropertyKeyTokenRecord )
            {
                ((ConsistencyReport.PropertyKeyTokenConsistencyReport) report)
                        .nameMultipleOwners( (PropertyKeyTokenRecord) record );
            }
            else if ( record instanceof DynamicRecord )
            {
                report.nameMultipleOwners( (DynamicRecord) record );
            }
        }
    }

    static class PropertyKey extends NameOwner<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport>
    {
        private final int id;

        PropertyKey( PropertyKeyTokenRecord record )
        {
            this.id = record.getIntId();
        }

        @Override
        RecordReference<PropertyKeyTokenRecord> record( RecordAccess records, PageCursorTracer cursorTracer )
        {
            return records.propertyKey( id, cursorTracer );
        }
    }

    static class LabelToken extends NameOwner<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport>
    {
        private final int id;

        LabelToken( LabelTokenRecord record )
        {
            this.id = record.getIntId();
        }

        @Override
        RecordReference<LabelTokenRecord> record( RecordAccess records, PageCursorTracer cursorTracer )
        {
            return records.label( id, cursorTracer );
        }
    }

    static class RelationshipTypeToken extends NameOwner<RelationshipTypeTokenRecord,ConsistencyReport.RelationshipTypeConsistencyReport>
    {
        private final int id;

        RelationshipTypeToken( RelationshipTypeTokenRecord record )
        {
            this.id = record.getIntId();
        }

        @Override
        RecordReference<RelationshipTypeTokenRecord> record( RecordAccess records, PageCursorTracer cursorTracer )
        {
            return records.relationshipType( id, cursorTracer );
        }
    }

    static class Unknown extends DynamicOwner<AbstractBaseRecord> implements RecordReference<AbstractBaseRecord>
    {
        private PendingReferenceCheck<AbstractBaseRecord> reporter;

        @Override
        RecordReference<AbstractBaseRecord> record( RecordAccess records, PageCursorTracer cursorTracer )
        {
            // Getting the record for this owner means that some other owner replaced it
            // that means that it isn't an orphan, so we skip this orphan check
            // and return a record for conflict check that always is ok (by skipping the check)
            this.markInCustody();
            return skipReference();
        }

        @Override
        public void checkOrphanage()
        {
            PendingReferenceCheck<AbstractBaseRecord> reporter = pop();
            if ( reporter != null )
            {
                reporter.checkReference( null, null, null );
            }
        }

        void markInCustody()
        {
            PendingReferenceCheck<AbstractBaseRecord> reporter = pop();
            if ( reporter != null )
            {
                reporter.skip();
            }
        }

        private synchronized PendingReferenceCheck<AbstractBaseRecord> pop()
        {
            try
            {
                return this.reporter;
            }
            finally
            {
                this.reporter = null;
            }
        }

        @Override
        public synchronized void dispatch( PendingReferenceCheck<AbstractBaseRecord> reporter )
        {
            this.reporter = reporter;
        }
    }

    private DynamicOwner()
    {
        // only internal subclasses
    }
}
