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
package org.neo4j.graphdb;

import java.util.Iterator;
import java.util.function.Supplier;

import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.FIRST;

/**
 * Find a dependency given a type.
 */
public interface DependencyResolver
{
    /**
     * Tries to resolve a dependency that matches a given class. No specific
     * {@link SelectionStrategy} is used, so the first encountered matching dependency will be returned.
     *
     *
     * @param type the type of {@link Class} that the returned instance must implement.
     * @param <T> the type that the returned instance must implement
     * @return the resolved dependency for the given type.
     * @throws IllegalArgumentException if no matching dependency was found.
     * @deprecated in next major version default selection strategy will be changed to more strict {@link DependencyResolver.SelectionStrategy#ONLY}
     */
    <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException;

    /**
     * Tries to resolve a dependency that matches a given class. All candidates are fed to the
     * {@code selector} which ultimately becomes responsible for making the choice between all available candidates.
     *
     * @param type the type of {@link Class} that the returned instance must implement.
     * @param selector {@link SelectionStrategy} which will make the choice of which one to return among
     * matching candidates.
     * @param <T> the type that the returned instance must implement
     * @return the resolved dependency for the given type.
     * @throws IllegalArgumentException if no matching dependency was found.
     */
    <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException;

    /**
     * Tries to resolve a dependencies that matches a given class.
     *
     * @param type the type of {@link Class} that the returned instances must implement.
     * @param <T> the type that the returned instance must implement
     * @return the list of resolved dependencies for the given type.
     */
    default <T> Iterable<? extends T> resolveTypeDependencies( Class<T> type )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    <T> Supplier<T> provideDependency( Class<T> type, SelectionStrategy selector );

    <T> Supplier<T> provideDependency( Class<T> type );

    /**
     * Responsible for making the choice between available candidates.
     */
    interface SelectionStrategy
    {
        /**
         * Given a set of candidates, select an appropriate one. Even if there are candidates this
         * method may throw {@link IllegalArgumentException} if there was no suitable candidate.
         *
         * @param type the type of items.
         * @param candidates candidates up for selection, where one should be picked. There might
         * also be no suitable candidate, in which case an exception should be thrown.
         * @param <T> the type of items
         * @return a suitable candidate among all available.
         * @throws IllegalArgumentException if no suitable candidate was found.
         */
        <T> T select( Class<T> type, Iterable<? extends T> candidates ) throws IllegalArgumentException;

        SelectionStrategy FIRST = new SelectionStrategy()
        {
            @Override
            public <T> T select( Class<T> type, Iterable<? extends T> candidates ) throws IllegalArgumentException
            {
                Iterator<? extends T> iterator = candidates.iterator();
                if ( !iterator.hasNext() )
                {
                    throw new IllegalArgumentException( "Could not resolve dependency of type:" + type.getName() );
                }
                return iterator.next();
            }
        };

        /**
         * Returns the one and only dependency, or throws.
         */
        SelectionStrategy ONLY = new SelectionStrategy()
        {
            @Override
            public <T> T select( Class<T> type, Iterable<? extends T> candidates ) throws IllegalArgumentException
            {
                Iterator<? extends T> iterator = candidates.iterator();
                if ( !iterator.hasNext() )
                {
                    throw new IllegalArgumentException( "Could not resolve dependency of type:" + type.getName() );
                }

                T only = iterator.next();

                if ( iterator.hasNext() )
                {
                    throw new IllegalArgumentException( "Multiple dependencies of type:" + type.getName() );
                }
                else
                {
                    return only;
                }
            }
        };
    }

    /**
     * Adapter for {@link DependencyResolver} which will select the first available candidate by default
     * for {@link #resolveDependency(Class)}.
     *
     * @deprecated in next major version default selection strategy will be changed to more strict {@link DependencyResolver.SelectionStrategy#ONLY}
     */
    abstract class Adapter implements DependencyResolver
    {
        @Override
        public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
        {
            return resolveDependency( type, FIRST );
        }

        @Override
        public <T> Supplier<T> provideDependency( final Class<T> type, final SelectionStrategy selector )
        {
            return () -> resolveDependency( type, selector );
        }

        @Override
        public <T> Supplier<T> provideDependency( final Class<T> type )
        {
            return () -> resolveDependency( type );
        }
    }
}
