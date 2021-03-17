/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.graphdb.Label;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.createBackup;
import static com.neo4j.backup.BackupTestUtil.restoreFromBackup;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.dbms.ClusterSystemGraphDbmsModel.DESIGNATED_SEEDER;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;

@TestDirectoryExtension
@ClusterExtension
@PageCacheExtension
public class DesignatedSeederIT
{
    private static final Label NODE_LABEL = Label.label( "test_node" );
    private static final String PROP_NAME = "bar";
    private static final String SOME_DB_NAME = "foo";
    private static final String FIRST_VALUE = "correct_db";
    private static final String SECOND_VALUE = "wrong_db";
    private final PageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private FileSystemAbstraction fsa;
    @Inject
    PageCache pageCache;

    private Cluster cluster;

    private Path firstValueBackup;
    private Path secondValueBackup;
    private Path differentStoreIdBackup;

    private int index;

    @BeforeAll
    void createBackups() throws Exception
    {
        // create cluster
        var config = ClusterConfig.clusterConfig()
                                  .withNumberOfCoreMembers( 2 )
                                  .withNumberOfReadReplicas( 0 );
        cluster = clusterFactory.createCluster( config );
        cluster.start();

        // create first value backup
        createDatabase( SOME_DB_NAME, cluster, true );
        writeNode( SOME_DB_NAME, FIRST_VALUE );
        var sourceMember = cluster.awaitLeader( SOME_DB_NAME );
        firstValueBackup = createBackup( sourceMember, newBackupDir(), SOME_DB_NAME );

        // create second value backup with same store id, but more transactions
        updateNode( SOME_DB_NAME, SECOND_VALUE );
        sourceMember = cluster.awaitLeader( SOME_DB_NAME );
        secondValueBackup = createBackup( sourceMember, newBackupDir(), SOME_DB_NAME );

        // create second value backup with different store id
        dropDatabase( SOME_DB_NAME, cluster );
        assertDatabaseEventuallyDoesNotExist( SOME_DB_NAME, cluster );
        createDatabase( SOME_DB_NAME, cluster, true );
        writeNode( SOME_DB_NAME, SECOND_VALUE );
        sourceMember = cluster.awaitLeader( SOME_DB_NAME );
        differentStoreIdBackup = createBackup( sourceMember, newBackupDir(), SOME_DB_NAME );

        cluster.shutdown();
    }

    @AfterEach
    void cleanup()
    {
        cluster.shutdown();
    }

    @Test
    void shouldSeedFromDesignatedSeederIfLocalStoreIsTooFarAhead() throws Exception
    {
        // create cluster
        var config = ClusterConfig.clusterConfig()
                                  .withNumberOfCoreMembers( 3 )
                                  .withNumberOfReadReplicas( 2 );
        cluster = clusterFactory.createCluster( config );
        var designatedSeeder = cluster.randomCoreMember( false ).orElseThrow( () -> new IllegalStateException( "Can't find any core member" ) );

        // given designated seeder has older backup restored than all other members
        for ( var member: cluster.allMembers() )
        {
            var backupToUse = member.equals( designatedSeeder ) ? firstValueBackup : secondValueBackup;
            restoreFromBackup( backupToUse, fsa, member, SOME_DB_NAME );
        }
        cluster.start();

        // when creating database from designated seeder
        createDatabaseWithDesignatedSeeder( designatedSeeder.serverId(), SOME_DB_NAME );

        // then all members should "rollback" to version from designated seeder
        assertDatabaseEventuallyStarted( SOME_DB_NAME, cluster );
        assertAllMembersHaveFirstValueDatabase( cluster );

        cluster.shutdown();
        assertThatAllMembersHaveSameDatabaseId( cluster, SOME_DB_NAME );
    }

    @Test
    void shouldSeedFromDesignatedSeederIfLocalStoreDoesNotExist() throws Exception
    {
        // create cluster
        var config = ClusterConfig.clusterConfig()
                                  .withNumberOfCoreMembers( 3 )
                                  .withNumberOfReadReplicas( 2 );
        cluster = clusterFactory.createCluster( config );

        // given only designated seeder have restored backup
        var designatedSeeder = cluster.randomCoreMember( false ).orElseThrow( () -> new IllegalStateException( "Can't find any core member" ) );
        restoreFromBackup( firstValueBackup, fsa, designatedSeeder, SOME_DB_NAME );

        // when creating database from designated seeder
        cluster.start();
        createDatabaseWithDesignatedSeeder( designatedSeeder.serverId(), SOME_DB_NAME );

        // then all members should get store from designated seeder
        assertDatabaseEventuallyStarted( SOME_DB_NAME, cluster );
        assertAllMembersHaveFirstValueDatabase( cluster );
        cluster.shutdown();
        assertThatAllMembersHaveSameDatabaseId( cluster, SOME_DB_NAME );
    }

    private static Stream<Arguments> instanceTypes()
    {
        return Stream.of( Arguments.of( ClusterMemberProvider.READ_REPLICA ),
                          Arguments.of( ClusterMemberProvider.CORE ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "instanceTypes" )
    void shouldUseDesignatedSeederStoreIfStoreIdIsDifferent( ClusterMemberProvider memberProvider ) throws Exception
    {
        // create cluster
        var config = ClusterConfig.clusterConfig()
                                  .withNumberOfCoreMembers( 3 )
                                  .withNumberOfReadReplicas( 1 );
        cluster = clusterFactory.createCluster( config );

        // restore a backup on the designated seeder
        var designatedSeeder = cluster.randomCoreMember( false ).orElseThrow( () -> new IllegalStateException( "Can't find any core member" ) );
        restoreFromBackup( firstValueBackup, fsa, designatedSeeder, SOME_DB_NAME );

        // restore the other backup on another member
        ClusterMember nonSeeder;
        do
        {
            nonSeeder = memberProvider.fromCluster( cluster );
        } while ( nonSeeder.equals( designatedSeeder ) );
        restoreFromBackup( differentStoreIdBackup, fsa, nonSeeder, SOME_DB_NAME );

        cluster.start();

        // when
        createDatabaseWithDesignatedSeeder( designatedSeeder.serverId(), SOME_DB_NAME );

        // then
        assertDatabaseEventuallyStarted( SOME_DB_NAME, cluster );
        assertAllMembersHaveFirstValueDatabase( cluster );
        cluster.shutdown();
        assertThatAllMembersHaveSameDatabaseId( cluster, SOME_DB_NAME );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "instanceTypes" )
    void shouldNotDeleteStoreIfDatabaseIdMatch( ClusterMemberProvider memberProvider ) throws Exception
    {
        // create cluster
        var config = ClusterConfig.clusterConfig()
                                  .withNumberOfCoreMembers( 3 )
                                  .withNumberOfReadReplicas( 1 );
        cluster = clusterFactory.createCluster( config );

        // restore a backup on the designated seeder
        var designatedSeeder = cluster.randomCoreMember( false ).orElseThrow( () -> new IllegalStateException( "Can't find any core member" ) );
        restoreFromBackup( firstValueBackup, fsa, designatedSeeder, SOME_DB_NAME );
        var originalDatabaseId = getDatabaseId( designatedSeeder, SOME_DB_NAME );
        cluster.start();

        // create database from seeder
        createDatabaseWithDesignatedSeeder( designatedSeeder.serverId(), SOME_DB_NAME );

        // then all the data is the same on all members
        assertDatabaseEventuallyStarted( SOME_DB_NAME, cluster );
        assertAllMembersHaveFirstValueDatabase( cluster );

        // create a backup from the seeder before adding new transaction
        var laterBackup = createBackup( designatedSeeder, newBackupDir(), SOME_DB_NAME );
        cluster.shutdown();
        var laterDatabaseId = getDatabaseId( designatedSeeder, SOME_DB_NAME );
        cluster.start();
        updateNode( SOME_DB_NAME, SECOND_VALUE );

        // new backup should have had its DatabaseId changed by the designated seeder
        assertThat( originalDatabaseId ).isNotEqualTo( laterDatabaseId );

        // restore later backup to a new member
        var newMember = memberProvider.newMember( cluster );
        restoreFromBackup( laterBackup, fsa, newMember, SOME_DB_NAME );
        var backupRestoreTime = getCreationDateOfDatabaseFolder( newMember );
        newMember.start();

        // then database should eventually be available and correct on all members
        assertDatabaseEventuallyStarted( SOME_DB_NAME, cluster );
        await().atMost( 30, SECONDS ).untilAsserted( () -> assertAllMembersHaveSecondValueDatabase( cluster ) );

        // and the new member have been able to catch up without replacing its store
        assertThat( backupRestoreTime ).isEqualTo( getCreationDateOfDatabaseFolder( newMember ) );
    }

    @Test
    void shouldBeAbleToRollOutDesignatedSeeder() throws Exception
    {
        // create cluster
        var nbrOfCores = 3;
        var config = ClusterConfig.clusterConfig()
                                  .withNumberOfCoreMembers( nbrOfCores )
                                  .withNumberOfReadReplicas( 2 );
        cluster = clusterFactory.createCluster( config );

        // restore backup to seeder
        var designatedSeeder = cluster.getCoreMemberByIndex( 0 );
        restoreFromBackup( firstValueBackup, fsa, designatedSeeder, SOME_DB_NAME );

        // start the cluster and create database with designated seeder
        cluster.start();
        createDatabaseWithDesignatedSeeder( designatedSeeder.serverId(), SOME_DB_NAME );
        assertDatabaseEventuallyStarted( SOME_DB_NAME, cluster );
        assertAllMembersHaveFirstValueDatabase( cluster );

        // roll the cluster until the last added core doesn't see any of the original cores
        for ( int i = 0; i < nbrOfCores + 1; i++ )
        {
            // add a new core
            var newCore = cluster.addCoreMemberWithIndex( nbrOfCores + i );
            newCore.start();

            // assert database eventually is available and correct on all running members
            assertDatabaseEventuallyStarted( SOME_DB_NAME, cluster );
            assertAllMembersHaveFirstValueDatabase( cluster );

            // remove an old core
            var coreToRemove = cluster.getCoreMemberByIndex( i );
            cluster.removeCoreMember( coreToRemove );
        }

        // read replica should be able to start without designated seeder or any other original member
        cluster.newReadReplica().start();
        assertDatabaseEventuallyStarted( SOME_DB_NAME, cluster );
        cluster.shutdown();
        assertThatAllMembersHaveSameDatabaseId( cluster, SOME_DB_NAME );
    }

    //TODO change to use the real command when implemented. (CREATE DATABASE foo SEED DATA FROM INSTANCE [SERVER ID])
    private void createDatabaseWithDesignatedSeeder( ServerId serverId, String dbName ) throws Exception
    {
        cluster.systemTx( ( db, tx ) ->
                          {
                              tx.execute( format( "CREATE DATABASE %s", dbName ) );
                              var node = tx.findNode( DATABASE_LABEL, DATABASE_NAME_PROPERTY, dbName );
                              node.setProperty( DESIGNATED_SEEDER, serverId.uuid().toString() );
                              tx.commit();
                          } );
//        cluster.systemTx( ( db, tx ) ->
//                          {
//                              tx.execute( format( "CREATE DATABASE %s SEED DATA FROM INSTANCE %s", dbName, serverId.uuid().toString() ) );
//                              tx.commit();
//                          } );
    }

    private void writeNode( String dbName, String propValue ) throws Exception
    {
        cluster.coreTx( dbName, ( db, tx ) -> {
            var node = tx.createNode( NODE_LABEL );
            node.setProperty( PROP_NAME, propValue );
            tx.commit();
        } );
    }

    private void updateNode( String dbName, String propValue ) throws Exception
    {
        cluster.coreTx( dbName, ( db, tx ) -> {
            var node = tx.findNodes( NODE_LABEL ).next();
            node.setProperty( PROP_NAME, propValue );
            tx.commit();
        } );
    }

    private Path newBackupDir()
    {
        return testDirectory.directory( format( "backup-%d", index++ ) );
    }

    private static void assertAllMembersHaveFirstValueDatabase( Cluster cluster )
    {
        for ( var member : cluster.allMembers() )
        {
            assertThat( memberHaveFirstValueDatabase( member ) ).isTrue();
            assertThat( memberHaveSecondValueDatabase( member ) ).isFalse();
        }
    }

    private void assertThatAllMembersHaveSameDatabaseId( Cluster cluster, String databaseName ) throws IOException
    {
        final var anyMember = cluster.allMembers().stream().findFirst().orElseThrow( () -> new IllegalStateException( "No members found" ) );
        final var databaseId = getDatabaseId( anyMember, databaseName );
        assertThat( databaseId ).isNotEmpty();
        for ( var member : cluster.allMembers() )
        {
            assertThat( databaseId ).isEqualTo( getDatabaseId( member, databaseName ) );
        }
    }

    private Optional<DatabaseId> getDatabaseId( ClusterMember clusterMember, String databaseName ) throws IOException
    {
        final var databaseLayout = getDatabaseLayout( clusterMember, databaseName );
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "test_tag" ) )
        {
            return MetaDataStore.getDatabaseId( pageCache, databaseLayout.metadataStore(), cursorTracer ).map( DatabaseIdFactory::from );
        }
    }

    private static void assertAllMembersHaveSecondValueDatabase( Cluster cluster )
    {
        for ( var member : cluster.allMembers() )
        {
            assertThat( memberHaveFirstValueDatabase( member ) ).isFalse();
            assertThat( memberHaveSecondValueDatabase( member ) ).isTrue();
        }
    }

    private static boolean memberHaveFirstValueDatabase( ClusterMember member )
    {
        try ( var tx = member.database( SOME_DB_NAME ).beginTx() )
        {
            var node = tx.findNode( NODE_LABEL, PROP_NAME, FIRST_VALUE );
            tx.commit();
            return node != null;
        }
    }

    private static boolean memberHaveSecondValueDatabase( ClusterMember member )
    {
        try ( var tx = member.database( SOME_DB_NAME ).beginTx() )
        {
            var node = tx.findNode( NODE_LABEL, PROP_NAME, SECOND_VALUE );
            tx.commit();
            return node != null;
        }
    }

    private Instant getCreationDateOfDatabaseFolder( ClusterMember clusterMember ) throws IOException
    {
        final DatabaseLayout databaseLayout = getDatabaseLayout( clusterMember, SOME_DB_NAME );
        return Files.readAttributes( databaseLayout.databaseDirectory(), BasicFileAttributes.class ).creationTime().toInstant();
    }

    private DatabaseLayout getDatabaseLayout( ClusterMember clusterMember, String databaseName )
    {
        Config config = Config.newBuilder().fromConfig( clusterMember.config() ).build();
        return Neo4jLayout.of( config ).databaseLayout( databaseName );
    }

    enum ClusterMemberProvider
    {
        CORE
                {
                    @Override
                    ClusterMember newMember( Cluster cluster )
                    {
                        return cluster.newCoreMember();
                    }

                    @Override
                    ClusterMember fromCluster( Cluster cluster )
                    {
                        return cluster.randomCoreMember( false ).orElseThrow( () -> new IllegalStateException( "Can't find any core member" ) );
                    }
                },
        READ_REPLICA
                {
                    @Override
                    ClusterMember newMember( Cluster cluster )
                    {
                        return cluster.newReadReplica();
                    }

                    @Override
                    ClusterMember fromCluster( Cluster cluster )
                    {
                        return cluster.findAnyReadReplica();
                    }
                };

        abstract ClusterMember newMember( Cluster cluster );

        abstract ClusterMember fromCluster( Cluster cluster );
    }
}
