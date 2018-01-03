/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.impl.BackupSupportingClassesFactoryProvider.getProvidersByPriority;
import static org.neo4j.backup.impl.BackupSupportingClassesFactoryProviderTest.allAvailableSupportingClassesFactories;
import static org.neo4j.backup.impl.BackupSupportingClassesFactoryProviderTest.findInstancesOf;

public class CommercialBackupSupportingClassesFactoryProviderTest
{
    @Test
    public void commercialProviderHasHigherPriorityThanCommunity()
    {
        BackupSupportingClassesFactoryProvider provider = getProvidersByPriority().findFirst().get();
        assertEquals( CommercialBackupSupportingClassesFactoryProvider.class, provider.getClass() );
    }

    @Test
    public void communityModuleIsStillDetectedToAvoidFalsePositive()
    {
        Collection<BackupSupportingClassesFactoryProvider> discoveredProviders =
                allAvailableSupportingClassesFactories();

        assertEquals( 1, findInstancesOf( BackupSupportingClassesFactoryProvider.class, discoveredProviders ).size() );
    }
}
