/*
 * Copyright (c) 2002-2019 "Neo Technology,"
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
package org.neo4j.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.function.ThrowingSupplier;

/**
 * Just like {@link Iterator}, but with the addition that {@link #hasNext()} and {@link #next()} can
 * be declared to throw a checked exception.
 *
 * @param <T> type of items in this iterator.
 * @param <EXCEPTION> type of exception thrown from {@link #hasNext()} and {@link #next()}.
 */
public interface RawIterator<T,EXCEPTION extends Exception>
{
    boolean hasNext() throws EXCEPTION;

    T next() throws EXCEPTION;

    default void remove()
    {
        throw new UnsupportedOperationException();
    }

    RawIterator<Object,Exception> EMPTY = RawIterator.of();

    @SuppressWarnings( "unchecked" )
    static <T, EXCEPTION extends Exception> RawIterator<T,EXCEPTION> empty()
    {
        return (RawIterator<T,EXCEPTION>) EMPTY;
    }

    static <T, EX extends Exception> RawIterator<T, EX> of( T ... values )
    {
        return new RawIterator<T,EX>()
        {
            private int position;

            @Override
            public boolean hasNext() throws EX
            {
                return position < values.length;
            }

            @Override
            public T next() throws EX
            {
                if ( hasNext() )
                {
                    return values[position++];
                }
                throw new NoSuchElementException();
            }
        };
    }

    /**
     * Create a raw iterator from the provided {@link ThrowingSupplier} - the iterator will end
     * when the supplier returns null.
     */
    static <T, EX extends Exception> RawIterator<T, EX> from( ThrowingSupplier<T, EX> supplier )
    {
        return new PrefetchingRawIterator<T,EX>()
        {
            @Override
            protected T fetchNextOrNull() throws EX
            {
                return supplier.get();
            }
        };
    }

    /**
     * Create a raw iterator from a regular iterator, assuming no exceptions are being thrown
     */
    static <T, EX extends Exception> RawIterator<T, EX> wrap( final Iterator<T> iterator )
    {
        return new RawIterator<T,EX>()
        {
            @Override
            public boolean hasNext() throws EX
            {
                return iterator.hasNext();
            }

            @Override
            public T next() throws EX
            {
                return iterator.next();
            }
        };
    }
}
