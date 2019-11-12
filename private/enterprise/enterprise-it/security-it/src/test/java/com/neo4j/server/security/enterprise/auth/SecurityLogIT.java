/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.EnterpriseSecurityModule;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.isA;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class SecurityLogIT
{
    @Test
    public void shouldFailDatabaseCreationIfNotAbleToCreateSecurityLog()
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        // Will throw either an IOException or a runtime SecurityException when calling RotatingFileOutputStreamSupplier.openOutputFile()
        ClassGuardedAdversary adversary = new ClassGuardedAdversary( new CountingAdversary( 1, true ), SecurityLog.class );
        final AdversarialFileSystemAbstraction evilFileSystem = new AdversarialFileSystemAbstraction( adversary );

        final DatabaseManagementServiceBuilder builder =
                new TestEnterpriseDatabaseManagementServiceBuilder().setFileSystem( evilFileSystem ).setInternalLogProvider( logProvider ).impermanent()
                                                                    .setConfig( GraphDatabaseSettings.auth_enabled, true );

        // When
        RuntimeException runtimeException =
                Assertions.assertThrows( RuntimeException.class, () ->
                {
                    DatabaseManagementService managementService = builder.build();
                    managementService.database( DEFAULT_DATABASE_NAME );
                } );

        // Then
        assertThat( runtimeException.getMessage(), equalTo( "Unable to create security log." ) );
        assertThat( runtimeException.getCause(), anyOf( isA( IOException.class ), isA( SecurityException.class ) ) );

        logProvider.assertAtLeastOnce( inLog( EnterpriseSecurityModule.class ).error( containsString( "Unable to create security log." ),
                                                                                      anyOf( isA( IOException.class ), isA( SecurityException.class ) ) ) );
    }

}
