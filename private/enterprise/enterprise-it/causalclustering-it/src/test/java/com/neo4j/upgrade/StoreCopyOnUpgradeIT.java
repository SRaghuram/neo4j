/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.upgrade;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.logging.Level;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipThreadLeakageGuard;
import org.neo4j.test.extension.SuppressOutputExtension;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseHasStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.memberDatabaseState;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.lang.Integer.max;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.io.fs.FileUtils.copyDirectory;
import static org.neo4j.test.Race.throwing;

@ClusterExtension
@ExtendWith( SuppressOutputExtension.class )
@TestInstance( PER_METHOD )
@ResourceLock( Resources.SYSTEM_OUT )
@SkipThreadLeakageGuard
class StoreCopyOnUpgradeIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    private static String[] ERR_STOREID_MISMATCH = new String[] {"cannot join the cluster", "has a mismatching storeId"};
    private static String[] ERR_STORE_VERSION_MISMATCH = new String[] {"Store copy failed due to store version mismatch"};
    private static String[] ERR_DIFFERENT_FORMAT = new String[] {"Failed to start database with copied store", "different record format"};

    @BeforeEach
    void before() throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withRecordFormat( Extended_StandardV4_0.PARENT_FORMAT )
                .withSharedPrimaryParam( GraphDatabaseSettings.allow_upgrade, TRUE )
                .withSharedReadReplicaParam( GraphDatabaseSettings.allow_upgrade, TRUE )
                .withSharedPrimaryParam( GraphDatabaseSettings.store_internal_log_level, Level.INFO.name() )
                .withSharedReadReplicaParam( GraphDatabaseSettings.store_internal_log_level, Level.INFO.name() )
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();

        //CreateInitialData
        for ( int i = 0; i < 100; i++ )
        {
            doSomeWriteTransactions();
        }
    }

    @AfterEach
    void after()
    {
        cluster.shutdown();
    }

    @ParameterizedTest( name = "{2}" )
    @MethodSource( "shouldHandleStoreCopyOfOldStoreToNewVersionArgs" )
    void shouldHandleStoreCopyOfOldStoreToNewVersion( MemberFactory factory, boolean seed, String unusedDisplayName ) throws Exception
    {
        ClusterMember member = addAndStartMemberWithNewVersionUnderLoad( factory, seed, WithExtraAdditiveCapability.NAME );
        try
        {
            assertDatabaseHasStarted( DEFAULT_DATABASE_NAME, cluster );
        }
        catch ( AssertionError e )
        {
            Path logFile = member.config().get( GraphDatabaseSettings.store_internal_log_path );
            e.initCause( new Exception( cropLogToMessage( logFile, "Exception" ) ) ); //Decorate failure with exceptions from debug log
            throw e;
        }
    }

    @ParameterizedTest( name = "{2}" )
    @MethodSource( "shouldHandleStoreCopyOfOldStoreToNewVersionArgs" )
    void shouldHandleMultipleDatabasesOfDifferentVersion( MemberFactory factory, boolean seed, String unusedDisplayName ) throws Exception
    {
        String latestOfCurrentFormat = ""; //Empty means latest of what ever format you are on, with default for new db's
        CoreClusterMember newCore = cluster.newCoreMemberWithRecordFormat( latestOfCurrentFormat );
        newCore.start();

        //Switch leader to ensure the new DB will be default format
        CausalClusteringTestHelpers.switchLeaderTo( cluster, newCore );
        cluster.systemTx( ( db, tx ) -> tx.execute( "CREATE DATABASE DBOnDefaultFormat" ) );

        ClusterMember member = addAndStartMemberWithNewVersionUnderLoad( factory, seed, latestOfCurrentFormat );
        try
        {
            assertDatabaseHasStarted( DEFAULT_DATABASE_NAME, cluster );
        }
        catch ( AssertionError e )
        {
            Path logFile = member.config().get( GraphDatabaseSettings.store_internal_log_path );
            e.initCause( new Exception( cropLogToMessage( logFile, "Exception" ) ) ); //Decorate failure with exceptions from debug log
            throw e;
        }
    }

    private static Stream<Arguments> shouldHandleStoreCopyOfOldStoreToNewVersionArgs()
    {
        return Stream.of(
                Arguments.of( MemberFactory.CORE, false, "Core without seed" ),
                Arguments.of( MemberFactory.CORE, true, "Core with seed" ),
                Arguments.of( MemberFactory.READ_REPLICA, false, "Read replica without seed" ),
                Arguments.of( MemberFactory.READ_REPLICA, true, "Read replica with seed" ) );
    }

    @ParameterizedTest( name = "{3}" )
    @MethodSource( "shouldNotHandleStoreCopyOfOldStoreToNewVersionCrossStoreFamilyArgs" )
    void shouldNotHandleStoreCopyOfOldStoreToNewVersionCrossStoreFamily( MemberFactory factory, boolean seed, String[] expectedErrorMessage,
            String unusedDisplayName ) throws Exception
    {
        ClusterMember member = addAndStartMemberWithNewVersionUnderLoad( factory, seed, WithExtraIncompatibleCapability.NAME );
        Path logFile = member.config().get( GraphDatabaseSettings.store_internal_log_path );
        String exceptionsFromLog = cropLogToMessage( logFile, "Exception" );
        assertThat( memberDatabaseState( member, DEFAULT_DATABASE_NAME ) ).as( exceptionsFromLog ).isEqualTo( STOPPED );
        assertThat( exceptionsFromLog ).contains( expectedErrorMessage );
    }

    private static Stream<Arguments> shouldNotHandleStoreCopyOfOldStoreToNewVersionCrossStoreFamilyArgs()
    {
        return Stream.of(
//                Arguments.of( MemberFactory.CORE, false, ERR_DIFFERENT_FORMAT, "Core without seed" ), //Enters infinite start loop on expected exception
//                Arguments.of( MemberFactory.CORE, true, ERR_DIFFERENT_FORMAT, "Core with seed" ), //Enters infinite start loop on expected exception
                Arguments.of( MemberFactory.READ_REPLICA, false, ERR_STORE_VERSION_MISMATCH, "Read replica without seed" ),
                Arguments.of( MemberFactory.READ_REPLICA, true, ERR_STOREID_MISMATCH, "Read replica with seed" ) );
    }

    private interface MemberFactory
    {
        ClusterMember create( Cluster cluster, String storeFormat );
        MemberFactory READ_REPLICA = Cluster::newReadReplicaWithRecordFormat;
        MemberFactory CORE = Cluster::newCoreMemberWithRecordFormat;
    }

    ClusterMember addAndStartMemberWithNewVersionUnderLoad( MemberFactory factory, boolean seed, String recordFormat ) throws Exception
    {
        ClusterMember member = seed ? seededMember( factory, recordFormat ) : factory.create( cluster, recordFormat );

        AtomicBoolean done = new AtomicBoolean();
        Race race = new Race().withRandomStartDelays().withEndCondition( done::get );
        race.addContestant( throwing( () ->
        {
            member.start();
            doSomeWriteTransactions();
            member.shutdown();
            member.start();
            doSomeWriteTransactions();
            done.set( true );
        } ) );
        race.addContestants( max( Runtime.getRuntime().availableProcessors() - 1, 2 ), throwing( this::doSomeWriteTransactions ) );

        Path logFile = member.config().get( GraphDatabaseSettings.store_internal_log_path );
        try
        {
            race.go( 5, TimeUnit.MINUTES );
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( "Unexpected error. Full debug log: " + readFile( logFile ), t );
        }

        assertThat( done.get() ).as( cropLogToMessage( logFile, "Exception" ) ).isTrue();
        return member;
    }

    private ClusterMember seededMember( MemberFactory factory, String format ) throws IOException
    {
        CoreClusterMember seedMember = cluster.primaryMembers().iterator().next();
        seedMember.shutdown();
        ClusterMember newMember = factory.create( cluster, format );
        copyDirectory( seedMember.databaseLayout().databaseDirectory(), newMember.databaseLayout().databaseDirectory() );
        copyDirectory( seedMember.databaseLayout().getTransactionLogsDirectory(), newMember.databaseLayout().getTransactionLogsDirectory() );
        seedMember.start();
        return newMember;
    }

    void doSomeWriteTransactions() throws Exception
    {
        for ( int i = 0; i < 10; i++ )
        {
            cluster.primaryTx( GraphDatabaseSettings.DEFAULT_DATABASE_NAME, ( graphDatabaseFacade, transaction ) ->
            {
                transaction.createNode( label( "foo" ) ).createRelationshipTo( transaction.createNode( label( "bar" ) ), withName( "baz" ) );
                transaction.commit();
            } );
        }
    }

    private String cropLogToMessage( Path log, String message ) throws IOException
    {
        String[] logLines = readFile( log ).split( "\n" );
        StringBuilder sb = new StringBuilder();
        int includeLine = 0;
        for ( String line : logLines )
        {
            if ( line.contains( message ) )
            {
                includeLine = 20;
            }
            if ( includeLine-- > 0 )
            {
                sb.append( line ).append( "\n" );
            }
        }
        return sb.toString();
    }

    private String readFile( Path path ) throws IOException
    {
        File file = path.toFile();
        return file.exists() ? FileUtils.readFileToString( file, Charset.defaultCharset() ) : "";
    }

    abstract static class Extended_StandardV4_0 extends StandardV4_0
    {
        static final String PARENT_FORMAT = StandardV4_0.NAME;
        private final Capability[] capabilities;
        private final String storeVersion;

        Extended_StandardV4_0( boolean additive )
        {
            capabilities = ArrayUtil.concat( super.capabilities(), new ExtraStorageCapability( additive ) );
            storeVersion = additive ? "4.0-eac" : "4.0-eic";
        }

        @Override
        public String storeVersion()
        {
            return storeVersion;
        }

        @Override
        public Capability[] capabilities()
        {
            return capabilities;
        }
    }

    static class WithExtraAdditiveCapability extends Extended_StandardV4_0
    {
        public static final String NAME = "standard_with_extra_additive_capabilities";
        static final RecordFormats INSTANCE = new WithExtraAdditiveCapability();

        WithExtraAdditiveCapability()
        {
            super( true );
        }
    }

    static class WithExtraIncompatibleCapability extends Extended_StandardV4_0
    {
        public static final String NAME = "standard_with_extra_incompatible_capabilities";
        static final RecordFormats INSTANCE = new WithExtraIncompatibleCapability();

        WithExtraIncompatibleCapability()
        {
            super( false );
        }
    }

    @ServiceProvider
    public static class WithExtraAdditiveCapabilityFactory implements RecordFormats.Factory
    {
        @Override
        public String getName()
        {
            return WithExtraAdditiveCapability.NAME;
        }

        @Override
        public RecordFormats newInstance()
        {
            return WithExtraAdditiveCapability.INSTANCE;
        }
    }

    @ServiceProvider
    public static class WithExtraIncompatibleCapabilityFactory implements RecordFormats.Factory
    {
        @Override
        public String getName()
        {
            return WithExtraIncompatibleCapability.NAME;
        }

        @Override
        public RecordFormats newInstance()
        {
            return WithExtraIncompatibleCapability.INSTANCE;
        }
    }

    static class ExtraStorageCapability implements Capability
    {
        private final boolean additive;

        ExtraStorageCapability( boolean additive )
        {
            this.additive = additive;
        }

        @Override
        public boolean isType( CapabilityType type )
        {
            return CapabilityType.STORE.equals( type );
        }

        @Override
        public boolean isAdditive()
        {
            return additive;
        }
    }
}
