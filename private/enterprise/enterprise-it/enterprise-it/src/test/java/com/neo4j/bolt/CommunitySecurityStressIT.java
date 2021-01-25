/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.configuration.OnlineBackupSettings;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.harness.junit.rule.Neo4jRule;

public class CommunitySecurityStressIT extends SecurityStressTestBase
{
    @Rule
    public Neo4jRule db = new Neo4jRule()
            .withConfig( GraphDatabaseSettings.auth_enabled, true )
            .withConfig( OnlineBackupSettings.online_backup_enabled, false );

    @Before
    public void setup()
    {
        setupAdminDriver( db );
    }

    @Test
    public void shouldHandleConcurrentAuthenticationAndDropUser() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool( 3 );
        service.submit( createUserWork );
        service.submit( deleteUserWork );
        service.submit( () -> defaultDbWork( db, Set.of( authenticationError ) ) );

        service.awaitTermination( 30, TimeUnit.SECONDS );

        assertNoUnexpectedErrors();
    }
}
