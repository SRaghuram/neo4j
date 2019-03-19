/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.enterprise.log.SecurityLog;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.isA;
import static org.neo4j.kernel.configuration.Settings.TRUE;
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

        final GraphDatabaseBuilder builder =
                new TestCommercialGraphDatabaseFactory()
                        .setFileSystem( evilFileSystem )
                        .setInternalLogProvider( logProvider )
                        .newImpermanentDatabaseBuilder()
                        .setConfig( GraphDatabaseSettings.auth_enabled, TRUE );

        // When
        RuntimeException runtimeException =
                Assertions.assertThrows( RuntimeException.class, builder::newGraphDatabase );

        // Then
        assertThat( runtimeException.getMessage(), equalTo( "Failed to load security module.") );
        assertThat( runtimeException.getCause(), instanceOf( IOException.class ) );

        logProvider.assertAtLeastOnce( inLog( EnterpriseEditionModule.class )
                .error( containsString( "Failed to load security module." ), isA( IOException.class )  ) );
    }

}