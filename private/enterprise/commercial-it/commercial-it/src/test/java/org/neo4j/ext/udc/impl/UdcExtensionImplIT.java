/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ext.udc.impl;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.ext.udc.UdcSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;
import org.neo4j.util.concurrent.RecentK;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.ext.udc.UdcConstants.DATABASE_MODE;
import static org.neo4j.ext.udc.UdcConstants.EDITION;
import static org.neo4j.ext.udc.UdcConstants.MAC;
import static org.neo4j.ext.udc.UdcConstants.REGISTRATION;
import static org.neo4j.ext.udc.UdcConstants.SOURCE;
import static org.neo4j.ext.udc.UdcConstants.TAGS;
import static org.neo4j.ext.udc.UdcConstants.USER_AGENTS;
import static org.neo4j.ext.udc.UdcConstants.VERSION;

/**
 * Unit testing for the UDC kernel extension.
 * <p>
 * The {@link UdcExtension} is loaded when a new graph database is instantiated.
 */
@ExtendWith( {SuppressOutputExtension.class, TestDirectoryExtension.class} )
class UdcExtensionImplIT
{
    private static final String VersionPattern = "(\\d\\.\\d+(([.-]).*)?)|(dev)";
    private static final Predicate<Integer> IS_ZERO = value -> value == 0;
    private static final Predicate<Integer> IS_GREATER_THAN_ZERO = value -> value > 0;

    @Inject
    private TestDirectory testDirectory;

    private PingerServer server;
    private Map<String,String> config;
    private GraphDatabaseService db;

    @BeforeEach
    void beforeEach() throws Exception
    {
        UdcTimerTask.successCounts.clear();
        UdcTimerTask.failureCounts.clear();
        server = new PingerServer();

        config = new HashMap<>();
        config.put( UdcSettings.first_delay.name(), "100" );
        config.put( UdcSettings.udc_host.name(), SocketAddress.format( server.getHost(), server.getPort() ) );
    }

    @AfterEach
    void afterEach()
    {
        IOUtils.closeAllSilently( server );
        if ( db != null )
        {
            db.shutdown();
        }
    }

    /**
     * Expect the counts to be initialized.
     */
    @Test
    void shouldLoadWhenNormalGraphdbIsCreated() throws Exception
    {
        // When
        db = createDatabase( emptyMap() );

        // Then, when the UDC extension successfully loads, it initializes the attempts count to 0
        assertGotSuccessWithRetry( IS_ZERO );
    }

    /**
     * Expect separate counts for each db.
     */
    @Test
    void shouldLoadForEachCreatedGraphdb()
    {
        var db1 = createDatabase( testDirectory.directory( "first-db" ), emptyMap() );
        var db2 = createDatabase( testDirectory.directory( "second-db" ), emptyMap() );

        try
        {
            assertThat( UdcTimerTask.successCounts.keySet(), hasSize( 2 ) );
        }
        finally
        {
            db1.shutdown();
            db2.shutdown();
        }
    }

    @Test
    void shouldRecordFailuresWhenThereIsNoServer() throws Exception
    {
        // When
        db = new TestGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.directory( "should-record-failures" ) )
                .setConfig( UdcSettings.first_delay, "100" )
                .setConfig( UdcSettings.udc_host, "127.0.0.1:1" )
                .newGraphDatabase();

        // Then
        assertGotFailureWithRetry( IS_GREATER_THAN_ZERO );
    }

    @Test
    void shouldRecordSuccessesWhenThereIsAServer() throws Exception
    {
        // When
        db = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertGotFailureWithRetry( IS_ZERO );
    }

    @Test
    void shouldBeAbleToSpecifySourceWithConfig() throws Exception
    {
        // When
        config.put( UdcSettings.udc_source.name(), "unit-testing" );
        db = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "unit-testing", server.getQueryMap().get( SOURCE ) );
    }

    @Test
    void shouldRecordDatabaseMode() throws Exception
    {
        // When
        db = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "single", server.getQueryMap().get( DATABASE_MODE ) );
    }

    @Test
    void shouldBeAbleToReadDefaultRegistration() throws Exception
    {
        // When
        db = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "unreg", server.getQueryMap().get( REGISTRATION ) );
    }

    @Test
    void shouldBeAbleToDetermineTestTagFromClasspath() throws Exception
    {
        // When
        db = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test,appserver,web", server.getQueryMap().get( TAGS ) );
    }

    @Test
    void shouldBeAbleToDetermineEditionFromClasspath() throws Exception
    {
        // When
        db = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( Edition.COMMERCIAL.toString(), server.getQueryMap().get( EDITION ) );
    }

    @Test
    void shouldBeAbleToDetermineUserAgent() throws Exception
    {
        // Given
        db = createDatabase( config );

        // When
        makeRequestWithAgent( "test/1.0" );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertEquals( "test/1.0", server.getQueryMap().get( USER_AGENTS ) );
    }

    @Test
    void shouldBeAbleToDetermineUserAgents() throws Exception
    {
        // Given
        db = createDatabase( config );

        // When
        makeRequestWithAgent( "test/1.0" );
        makeRequestWithAgent( "foo/bar" );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        var userAgents = server.getQueryMap().get( USER_AGENTS );
        assertThat( userAgents, containsString( "test/1.0" ) );
        assertThat( userAgents, containsString( "foo/bar" ) );
    }

    @Test
    void shouldIncludeMacAddressInConfig() throws Exception
    {
        // When
        db = createDatabase( config );

        // Then
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        assertNotNull( server.getQueryMap().get( MAC ) );
    }

    @Test
    void shouldIncludePrefixedSystemProperties() throws Exception
    {
        withSystemProperty( UdcConstants.UDC_PROPERTY_PREFIX + ".test", "udc-property", () ->
        {
            withSystemProperty( "os.test", "os-property", () ->
            {
                db = createDatabase( config );
                assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
                assertEquals( "udc-property", server.getQueryMap().get( "test" ) );
                assertEquals( "os-property", server.getQueryMap().get( "os.test" ) );
                return null;
            } );
            return null;
        } );
    }

    @Test
    void shouldNotIncludeDistributionForWindows() throws Exception
    {
        withSystemProperty( "os.name", "Windows", () ->
        {
            db = createDatabase( config );
            assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
            assertEquals( UdcConstants.UNKNOWN_DIST, server.getQueryMap().get( "dist" ) );
            return null;
        } );
    }

    @Test
    void shouldIncludeDistributionForLinux() throws Exception
    {
        if ( !SystemUtils.IS_OS_LINUX )
        {
            return;
        }
        db = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );

        assertEquals( DefaultUdcInformationCollector.searchForPackageSystems(), server.getQueryMap().get( "dist" ) );
    }

    @Test
    void shouldNotIncludeDistributionForMacOS() throws Exception
    {
        withSystemProperty( "os.name", "Mac OS X", () ->
        {
            db = createDatabase( config );
            assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
            assertEquals( UdcConstants.UNKNOWN_DIST, server.getQueryMap().get( "dist" ) );
            return null;
        } );
    }

    @Test
    void shouldIncludeVersionInConfig() throws Exception
    {
        db = createDatabase( config );
        assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
        var version = server.getQueryMap().get( VERSION );
        assertThat( version, matchesRegex( VersionPattern ) );
    }

    @Test
    void shouldOverrideSourceWithSystemProperty() throws Exception
    {
        withSystemProperty( UdcSettings.udc_source.name(), "overridden", () ->
        {
            db = createDatabase( testDirectory.directory( "db-with-property" ), config );
            assertGotSuccessWithRetry( IS_GREATER_THAN_ZERO );
            var source = server.getQueryMap().get( SOURCE );
            assertEquals( "overridden", source );
            return null;
        } );
    }

    @Test
    void shouldMatchAllValidVersions()
    {
        assertThat( "1.8.M07", matchesRegex( VersionPattern ) );
        assertThat( "1.8.RC1", matchesRegex( VersionPattern ) );
        assertThat( "1.8.GA", matchesRegex( VersionPattern ) );
        assertThat( "1.8", matchesRegex( VersionPattern ) );
        assertThat( "1.9", matchesRegex( VersionPattern ) );
        assertThat( "1.9-SNAPSHOT", matchesRegex( VersionPattern ) );
        assertThat( "2.0-SNAPSHOT", matchesRegex( VersionPattern ) );
        assertThat( "1.9.M01", matchesRegex( VersionPattern ) );
        assertThat( "1.10", matchesRegex( VersionPattern ) );
        assertThat( "1.10-SNAPSHOT", matchesRegex( VersionPattern ) );
        assertThat( "1.10.M01", matchesRegex( VersionPattern ) );
    }

    @Test
    void shouldFilterPlusBuildNumbers()
    {
        assertEquals( "1.9.0-M01", DefaultUdcInformationCollector.filterVersionForUDC( "1.9.0-M01+00001" ) );
    }

    @Test
    void shouldNotFilterSnapshotBuildNumbers()
    {
        assertEquals( "2.0-SNAPSHOT", DefaultUdcInformationCollector.filterVersionForUDC( "2.0-SNAPSHOT" ) );

    }

    @Test
    void shouldNotFilterReleaseBuildNumbers()
    {
        assertEquals( "1.9", DefaultUdcInformationCollector.filterVersionForUDC( "1.9" ) );
    }

    @Test
    void shouldUseTheCustomConfiguration()
    {
        // Given
        config.put( UdcSettings.udc_source.name(), "my_source" );
        config.put( UdcSettings.udc_registration_key.name(), "my_key" );

        // When
        db = createDatabase( config );

        // Then
        var config = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( Config.class );

        assertEquals( "my_source", config.get( UdcSettings.udc_source ) );
        assertEquals( "my_key", config.get( UdcSettings.udc_registration_key ) );
    }

    private static void assertGotSuccessWithRetry( Predicate<Integer> predicate ) throws Exception
    {
        assertGotPingWithRetry( UdcTimerTask.successCounts, predicate );
    }

    private static void assertGotFailureWithRetry( Predicate<Integer> predicate ) throws Exception
    {
        assertGotPingWithRetry( UdcTimerTask.failureCounts, predicate );
    }

    private static void assertGotPingWithRetry( Map<String,Integer> counts, Predicate<Integer> predicate ) throws Exception
    {
        for ( int i = 0; i < 100; i++ )
        {
            Collection<Integer> countValues = counts.values();
            Integer count = countValues.iterator().next();
            if ( predicate.test( count ) )
            {
                return;
            }
            Thread.sleep( 200 );
        }
        fail();
    }

    private static GraphDatabaseService createDatabase( Map<String,String> config )
    {
        return createDatabase( null, config );
    }

    private static GraphDatabaseService createDatabase( File storeDir, Map<String,String> config )
    {
        TestCommercialGraphDatabaseFactory factory = new TestCommercialGraphDatabaseFactory();
        FormattedLogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );
        factory.setInternalLogProvider( logProvider );
        factory.setUserLogProvider( logProvider );
        GraphDatabaseBuilder graphDatabaseBuilder =
                (storeDir != null) ? factory.newImpermanentDatabaseBuilder( storeDir )
                                   : factory.newImpermanentDatabaseBuilder();
        if ( config != null )
        {
            graphDatabaseBuilder.setConfig( config );
        }

        return graphDatabaseBuilder.newGraphDatabase();
    }

    private void makeRequestWithAgent( String agent )
    {
        RecentK<String> clients =
                ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( UsageData.class )
                        .get( UsageDataKeys.clientNames );
        clients.add( agent );
    }

    private static void withSystemProperty( String name, String value, Callable<Void> block ) throws Exception
    {
        String original = System.getProperty( name );
        System.setProperty( name, value );
        try
        {
            block.call();
        }
        finally
        {
            if ( original == null )
            {
                System.clearProperty( name );
            }
            else
            {
                System.setProperty( name, original );
            }
        }
    }
}
