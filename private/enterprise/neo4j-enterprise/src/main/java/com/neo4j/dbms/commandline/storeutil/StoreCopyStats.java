/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.logging.Log;
import org.neo4j.values.storable.Value;

import static java.lang.Math.max;
import static java.lang.String.format;

class StoreCopyStats
{
    private static final int MAX_LOG_LENGTH = 120;

    private final long startTime;
    private final Log log;
    final LongAdder count = new LongAdder();
    final LongAdder unused = new LongAdder();
    final LongAdder removed = new LongAdder();

    StoreCopyStats( Log log )
    {
        this.log = log;
        startTime = System.nanoTime();
    }

    void addCorruptToken( String type, int id )
    {
        log.error( "%s(%d): Missing token name", type, id );
    }

    void brokenPropertyToken( String type, PrimitiveRecord record, Value newPropertyValue, int keyIndexId )
    {
        String value = newPropertyValue.toString();
        log.error( "%s(%d): Ignoring property with missing token(%d). Value of the property is %s.",
                type, record.getId(), keyIndexId, trimToMaxLength( value ) );
    }

    void brokenPropertyChain( String type, PrimitiveRecord record, Exception e )
    {
        log.error( format( "%s(%d): Ignoring broken property chain.", type, record.getId() ), e );
    }

    void circularPropertyChain( String type, PrimitiveRecord record, PropertyRecord circleCompleter )
    {
        log.error( format( "%s(%d): Ignoring circular property chain %s.", type, record.getId(), circleCompleter ) );
    }

    void brokenRecord( String type, long id, Exception e )
    {
        log.error( format( "%s(%d): Ignoring broken record.", type, id ), e );
    }

    void printSummary()
    {
        long seconds = TimeUnit.NANOSECONDS.toSeconds( System.nanoTime() - startTime );
        long count = this.count.sum();
        long unused = this.unused.sum();
        long removed = this.removed.sum();

        log.info( "Import summary: Copying of %d records took %d seconds (%d rec/s). Unused Records %d (%d%%) Removed Records %d (%d%%)",
                count, seconds, count / max( 1L, seconds ), unused, percent( unused, count ), removed, percent( removed, count ));
    }

    void invalidIndex( IndexDescriptor indexDescriptor, Exception e )
    {
        log.error( format( "Unable to format statement for index '%s'%n", indexDescriptor.getName() ), e );
    }

    void invalidConstraint( ConstraintDescriptor constraintDescriptor, Exception e )
    {
        log.error( format( "Unable to format statement for constraint '%s'%n", constraintDescriptor.getName() ), e );
    }

    private static int percent( long part, long total )
    {
        if ( total == 0L )
        {
            return 0;
        }
        return (int) (100 * ((double) part) / total);
    }

    private static String trimToMaxLength( String value )
    {
        return value.length() <= MAX_LOG_LENGTH ? value : value.substring( 0, MAX_LOG_LENGTH ) + "..";
    }
}
