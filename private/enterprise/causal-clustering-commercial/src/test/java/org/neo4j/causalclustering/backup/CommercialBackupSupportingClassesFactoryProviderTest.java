/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.backup;

import org.junit.Test;

import java.util.Collection;

import org.neo4j.backup.BackupSupportingClassesFactoryProvider;
import org.neo4j.backup.BackupSupportingClassesFactoryProviderTest;
import org.neo4j.backup.CommunityBackupSupportingClassesFactoryProvider;

import static org.junit.Assert.assertEquals;
import static org.neo4j.backup.BackupSupportingClassesFactoryProvider.findBestProvider;

public class CommercialBackupSupportingClassesFactoryProviderTest
{
    @Test
    public void commercialProviderHasHigherPriorityThanCommunity()
    {
        assertEquals( CommercialBackupSupportingClassesFactoryProvider.class, findBestProvider().get().getClass() );
    }

    @Test
    public void communityModuleIsStillDetectedToAvoidFalsePositive()
    {
        Collection<BackupSupportingClassesFactoryProvider> discoveredProviders =
                BackupSupportingClassesFactoryProviderTest.allAvailableSupportingClassesFactories();

        assertEquals( 1, BackupSupportingClassesFactoryProviderTest.findInstancesOf( CommunityBackupSupportingClassesFactoryProvider.class,
                discoveredProviders ).size() );
    }
}
