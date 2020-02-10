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
package org.neo4j.consistency.checking;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DirectRecordReference;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;

public class PropertyRecordCheck
        implements RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
{
    @Override
    public void check( PropertyRecord record,
                       CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                       RecordAccess records, PageCursorTracer cursorTracer )
    {
        if ( !record.inUse() )
        {
            return;
        }
        for ( PropertyField field : PropertyField.values() )
        {
            field.checkConsistency( record, engine, records, cursorTracer );
        }
        for ( PropertyBlock block : record )
        {
            checkDataBlock( record.getId(), block, engine, records, cursorTracer );
        }
    }

    public static void checkDataBlock( long propertyRecordId, PropertyBlock block,
                                 CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                 RecordAccess records, PageCursorTracer cursorTracer )
    {
        if ( block.getKeyIndexId() < 0 )
        {
            engine.report().invalidPropertyKey( block );
        }
        else
        {
            engine.comparativeCheck( records.propertyKey( block.getKeyIndexId(), cursorTracer ), propertyKey( block ) );
        }
        PropertyType type = block.forceGetType();
        if ( type == null )
        {
            engine.report().invalidPropertyType( block );
        }
        else
        {
            switch ( type )
            {
            case STRING:
                engine.comparativeCheck( records.string( block.getSingleValueLong(), cursorTracer ),
                                         DynamicReference.string( block ) );
                break;
            case ARRAY:
                engine.comparativeCheck( records.array( block.getSingleValueLong(), cursorTracer ), DynamicReference.array( block ) );
                break;
            default:
                try
                {
                    type.value( block, null, cursorTracer );
                }
                catch ( Exception e )
                {
                    engine.report().invalidPropertyValue( propertyRecordId, block.getKeyIndexId() );
                }
                break;
            }
        }
    }

    public enum PropertyField implements
            RecordField<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>,
            ComparativeRecordChecker<PropertyRecord, PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
    {
        PREV( Record.NO_PREVIOUS_PROPERTY )
        {
            @Override
            public long valueFrom( PropertyRecord record )
            {
                return record.getPrevProp();
            }

            @Override
            public long otherReference( PropertyRecord record )
            {
                return record.getNextProp();
            }

            @Override
            public void notInUse( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.prevNotInUse( property );
            }

            @Override
            public void noBackReference( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.previousDoesNotReferenceBack( property );
            }
        },
        NEXT( Record.NO_NEXT_PROPERTY )
        {
            @Override
            public long valueFrom( PropertyRecord record )
            {
                return record.getNextProp();
            }

            @Override
            public long otherReference( PropertyRecord record )
            {
                return record.getPrevProp();
            }

            @Override
            public void notInUse( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.nextNotInUse( property );
            }

            @Override
            public void noBackReference( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.nextDoesNotReferenceBack( property );
            }
        };
        private final Record NONE;

        PropertyField( Record none )
        {
            this.NONE = none;
        }

        public abstract long otherReference( PropertyRecord record );

        @Override
        public void checkConsistency( PropertyRecord record, CheckerEngine<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> engine,
                RecordAccess records, PageCursorTracer cursorTracer )
        {
            if ( !NONE.is( valueFrom( record ) ) )
            {
                PropertyRecord prop = records.cacheAccess().client().getPropertyFromCache( valueFrom( record ) );
                if ( prop == null )
                {
                    engine.comparativeCheck( records.property( valueFrom( record ), cursorTracer ), this );
                }
                else
                {
                    engine.comparativeCheck( new DirectRecordReference<>( prop, records, cursorTracer ), this );
                }
            }
        }

        @Override
        public void checkReference( PropertyRecord record, PropertyRecord referred,
                CheckerEngine<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> engine, RecordAccess records, PageCursorTracer cursorTracer )
        {
            if ( !referred.inUse() )
            {
                notInUse( engine.report(), referred );
            }
            else
            {
                if ( otherReference( referred ) != record.getId() )
                {
                    noBackReference( engine.report(), referred );
                }
            }
        }

        abstract void notInUse( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property );

        public abstract void noBackReference( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property );
    }

    private static ComparativeRecordChecker<PropertyRecord, PropertyKeyTokenRecord, ConsistencyReport.PropertyConsistencyReport>
    propertyKey( final PropertyBlock block )
    {
        return new ComparativeRecordChecker<>()
        {
            @Override
            public void checkReference( PropertyRecord record, PropertyKeyTokenRecord referred,
                    CheckerEngine<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> engine, RecordAccess records, PageCursorTracer cursorTracer )
            {
                if ( !referred.inUse() )
                {
                    engine.report().keyNotInUse( block, referred );
                }
            }

            @Override
            public String toString()
            {
                return "PROPERTY_KEY";
            }
        };
    }

    private abstract static class DynamicReference implements
            ComparativeRecordChecker<PropertyRecord, DynamicRecord, ConsistencyReport.PropertyConsistencyReport>
    {
        final PropertyBlock block;

        private DynamicReference( PropertyBlock block )
        {
            this.block = block;
        }

        public static DynamicReference string( PropertyBlock block )
        {
            return new DynamicReference( block )
            {
                @Override
                void notUsed( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.stringNotInUse( block, value );
                }

                @Override
                void empty( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.stringEmpty( block, value );
                }
            };
        }

        public static DynamicReference array( PropertyBlock block )
        {
            return new DynamicReference( block )
            {
                @Override
                void notUsed( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.arrayNotInUse( block, value );
                }

                @Override
                void empty( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.arrayEmpty( block, value );
                }
            };
        }

        @Override
        public void checkReference( PropertyRecord record, DynamicRecord referred,
                CheckerEngine<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> engine, RecordAccess records, PageCursorTracer cursorTracer )
        {
            if ( !referred.inUse() )
            {
                notUsed( engine.report(), referred );
            }
            else
            {
                if ( referred.getLength() <= 0 )
                {
                    empty( engine.report(), referred );
                }
            }
        }

        abstract void notUsed( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value );

        abstract void empty( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value );
    }
}
