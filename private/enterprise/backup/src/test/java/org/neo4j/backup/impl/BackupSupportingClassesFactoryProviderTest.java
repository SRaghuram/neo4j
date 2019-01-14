/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.Test;

import java.util.Collection;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.impl.BackupSupportingClassesFactoryProvider.getProvidersByPriority;

public class BackupSupportingClassesFactoryProviderTest
{
    @Test
    public void canLoadDefaultSupportingClassesFactory()
    {
        assertEquals( 1, findInstancesOf( BackupSupportingClassesFactoryProvider.class,
                allAvailableSupportingClassesFactories() ).size() );
        assertEquals( 2, allAvailableSupportingClassesFactories().size() );
    }

    @Test
    public void testDefaultModuleIsPrioritisedOverDummyModule()
    {
        assertEquals( BackupSupportingClassesFactoryProvider.class,
                getProvidersByPriority().findFirst().get().getClass() );
    }

    public static Collection<BackupSupportingClassesFactoryProvider> allAvailableSupportingClassesFactories()
    {
        return getProvidersByPriority().collect( toList() );
    }

    public static <DESIRED extends BackupSupportingClassesFactoryProvider> Collection<DESIRED> findInstancesOf(
            Class<DESIRED> desiredClass, Collection<? extends BackupSupportingClassesFactoryProvider> collection )
    {
        return collection
                .stream()
                .filter( isOfClass( desiredClass ) )
                .map( i -> (DESIRED) i )
                .collect( toList() );
    }

    private static Predicate<BackupSupportingClassesFactoryProvider> isOfClass(
            Class<? extends BackupSupportingClassesFactoryProvider> desiredClass )
    {
        return factory -> desiredClass.equals( factory.getClass() );
    }
}
