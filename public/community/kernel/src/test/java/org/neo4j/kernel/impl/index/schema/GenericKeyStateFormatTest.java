/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.FormatCompatibilityVerifier;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GenericKeyStateFormatTest extends FormatCompatibilityVerifier
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RandomRule randomRule = new RandomRule().withSeedForAllTests( 20051116 );

    private static final int NUMBER_OF_SLOTS = 2;
    private List<Value> values;

    @Before
    public void setup()
    {
        RandomValues rnd = randomRule.randomValues();
        values = new ArrayList<>();
        // ZONED_DATE_TIME_ARRAY
        values.add( rnd.nextDateTimeArray() );
        // LOCAL_DATE_TIME_ARRAY
        values.add( rnd.nextLocalDateTimeArray() );
        // DATE_ARRAY
        values.add( rnd.nextDateArray() );
        // ZONED_TIME_ARRAY
        values.add( rnd.nextTimeArray() );
        // LOCAL_TIME_ARRAY
        values.add( rnd.nextLocalTimeArray() );
        // DURATION_ARRAY
        values.add( rnd.nextDurationArray() );
        // TEXT_ARRAY
        values.add( rnd.nextStringArray() );
        // BOOLEAN_ARRAY
        values.add( rnd.nextBooleanArray() );
        // NUMBER_ARRAY (byte, short, int, long, float, double)
        values.add( rnd.nextByteArray() );
        values.add( rnd.nextShortArray() );
        values.add( rnd.nextIntArray() );
        values.add( rnd.nextLongArray() );
        values.add( rnd.nextFloatArray() );
        values.add( rnd.nextDoubleArray() );
        // ZONED_DATE_TIME
        values.add( rnd.nextDateTimeValue() );
        // LOCAL_DATE_TIME
        values.add( rnd.nextLocalDateTimeValue() );
        // DATE
        values.add( rnd.nextDateValue() );
        // ZONED_TIME
        values.add( rnd.nextTimeValue() );
        // LOCAL_TIME
        values.add( rnd.nextLocalTimeValue() );
        // DURATION
        values.add( rnd.nextDuration() );
        // TEXT
        values.add( rnd.nextTextValue() );
        // BOOLEAN
        values.add( rnd.nextBooleanValue() );
        // NUMBER (byte, short, int, long, float, double)
        values.add( rnd.nextByteValue() );
        values.add( rnd.nextShortValue() );
        values.add( rnd.nextIntValue() );
        values.add( rnd.nextLongValue() );
        values.add( rnd.nextFloatValue() );
        values.add( rnd.nextDoubleValue() );
        // todo GEOMETRY
        // todo GEOMETRY_ARRAY
    }

    @Override
    protected String zipName()
    {
        return "current-generic-key-state-format.zip";
    }

    @Override
    protected String storeFileName()
    {
        return "generic-key-state-store";
    }

    @Override
    protected void createStoreFile( File storeFile ) throws IOException
    {
        withCursor( storeFile, true, c -> {
            putFormatVersion( c );
            putData( c );
        } );
    }

    @Override
    protected void verifyFormat( File storeFile ) throws FormatViolationException, IOException
    {
        AtomicReference<FormatViolationException> exception = new AtomicReference<>();
        withCursor( storeFile, false, c ->
        {
            int major = c.getInt();
            int minor = c.getInt();
            GenericLayout layout = getLayout();
            if ( major != layout.majorVersion() || minor != layout.minorVersion() )
            {
                exception.set( new FormatViolationException( String.format( "Read format version %d.%d, but layout has version %d.%d",
                        major, minor, layout.majorVersion(), layout.minorVersion() ) ) );
            }
        } );
        if ( exception.get() != null )
        {
            throw exception.get();
        }
    }

    @Override
    protected void verifyContent( File storeFile ) throws IOException
    {
        withCursor( storeFile, false, c ->
        {
            readFormatVersion( c );
            verifyData( c );
        } );
    }

    private void putFormatVersion( PageCursor cursor )
    {
        GenericLayout layout = getLayout();
        int major = layout.majorVersion();
        cursor.putInt( major );
        int minor = layout.minorVersion();
        cursor.putInt( minor );
    }

    private void readFormatVersion( PageCursor c )
    {
        c.getInt(); // Major version
        c.getInt(); // Minor version
    }

    private void putData( PageCursor c )
    {
        GenericLayout layout = getLayout();
        CompositeGenericKey key = layout.newKey();
        for ( Value value : values )
        {
            key.initialize( 19570320 );
            for ( int i = 0; i < NUMBER_OF_SLOTS; i++ )
            {
                key.initFromValue( i, value, NativeIndexKey.Inclusion.NEUTRAL );
            }
            c.putInt( key.size() );
            layout.writeKey( c, key );
        }
    }

    private void verifyData( PageCursor c )
    {
        GenericLayout layout = getLayout();
        CompositeGenericKey into = layout.newKey();
        for ( Value value : values )
        {
            int keySize = c.getInt();
            layout.readKey( c, into, keySize );
            for ( Value readValue : into.asValues() )
            {
                assertEquals( value, readValue, "expected read value to be " + value + ", but was " + readValue );
            }
        }
    }

    private GenericLayout getLayout()
    {
        return new GenericLayout( NUMBER_OF_SLOTS );
    }

    private void withCursor( File storeFile, boolean create, Consumer<PageCursor> cursorConsumer ) throws IOException
    {
        OpenOption[] openOptions = create ?
                                   new OpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE} :
                                   new OpenOption[]{StandardOpenOption.WRITE};
        try ( PageCache pageCache = pageCacheRule.getPageCache( globalFs.get() );
              PagedFile pagedFile = pageCache.map( storeFile, pageCache.pageSize(), openOptions );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
        {
            cursor.next();
            cursorConsumer.accept( cursor );
        }
    }
}
