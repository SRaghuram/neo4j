/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.helper.TemporaryDatabase;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredException;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;
import static org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker.assertRecoveryIsNotRequired;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.id.IdType.ARRAY_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.NEOSTORE_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.kernel.impl.store.id.IdType.NODE_LABELS;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.SCHEMA;
import static org.neo4j.kernel.impl.store.id.IdType.STRING_BLOCK;

public class CoreBootstrapper
{
    private static final long FIRST_INDEX = 0L;
    private static final long FIRST_TERM = 0L;

    private final DatabaseService databaseService;
    private final TemporaryDatabase.Factory tempDatabaseFactory;
    private final File bootstrapDirectory;
    private final Map<String,DatabaseInitializer> databaseInitializers;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Config config;
    private final Log log;
    private final Monitors monitors;

    CoreBootstrapper( DatabaseService databaseService, TemporaryDatabase.Factory tempDatabaseFactory, File bootstrapDirectory,
            Map<String,DatabaseInitializer> databaseInitializers, FileSystemAbstraction fs, Config config, LogProvider logProvider, PageCache pageCache,
            Monitors monitors )
    {
        this.databaseService = databaseService;
        this.tempDatabaseFactory = tempDatabaseFactory;
        this.bootstrapDirectory = bootstrapDirectory; // typically: data/bootstrap/
        this.databaseInitializers = databaseInitializers;
        this.fs = fs;
        this.config = config;
        this.pageCache = pageCache;
        this.log = logProvider.getLog( getClass() );
        this.monitors = monitors;
    }

    public CoreSnapshot bootstrap( Set<MemberId> members ) throws Exception
    {
        if ( fs.fileExists( bootstrapDirectory ) )
        {
            throw new IllegalStateException( "Directory should not exist: " + bootstrapDirectory );
        }

        try
        {
            fs.mkdirs( bootstrapDirectory );
            return bootstrap0( members );
        }
        finally
        {
            fs.deleteRecursively( bootstrapDirectory );
        }
    }

    private CoreSnapshot bootstrap0( Set<MemberId> members ) throws Exception
    {
        CoreSnapshot coreSnapshot = new CoreSnapshot( FIRST_INDEX, FIRST_TERM );
        coreSnapshot.add( CoreStateFiles.RAFT_CORE_STATE, new RaftCoreState( new MembershipEntry( FIRST_INDEX, members ) ) );
        coreSnapshot.add( CoreStateFiles.SESSION_TRACKER, new GlobalSessionTrackerState() );

        for ( Map.Entry<String,? extends LocalDatabase> database : databaseService.registeredDatabases().entrySet() )
        {
            String databaseName = database.getKey();
            DatabaseLayout databaseLayout = database.getValue().databaseLayout();
            assert databaseName.equals( databaseLayout.getDatabaseName() );

            bootstrapDatabase( databaseName, databaseLayout );

            IdAllocationState idAllocationState = deriveIdAllocationState( databaseLayout );
            coreSnapshot.add( databaseName, CoreStateFiles.ID_ALLOCATION, idAllocationState );
            coreSnapshot.add( databaseName, CoreStateFiles.LOCK_TOKEN, new ReplicatedLockTokenState() );
        }

        return coreSnapshot;
    }

    private void bootstrapDatabase( String databaseName, DatabaseLayout databaseLayout ) throws IOException, RecoveryRequiredException
    {
        checkRecovered(databaseLayout);

        if ( !NeoStores.isStorePresent( pageCache, databaseLayout ) )
        {
            File databaseDirectory = databaseLayout.databaseDirectory();
            fs.deleteRecursively( databaseDirectory );

            String txLogsDirectoryName = "tx-logs";
            File txLogsAfterBootstrap = new File( databaseDirectory, txLogsDirectoryName );

            DatabaseLayout bootstrapDatabaseLayout = StoreLayout.of( bootstrapDirectory ).databaseLayout( "bootstrap.db" );
            File bootstrapDatabaseDirectory = bootstrapDatabaseLayout.databaseDirectory();
            initializeDatabase( bootstrapDatabaseDirectory, databaseInitializers.get( databaseName ), txLogsDirectoryName );

            fs.renameFile( bootstrapDatabaseDirectory, databaseDirectory );
            moveTransactionLogs( txLogsAfterBootstrap, config.get( GraphDatabaseSettings.logical_logs_location ) );
            if ( !fs.deleteFile( txLogsAfterBootstrap ) )
            {
                log.warn( "Could not delete: " + txLogsAfterBootstrap );
            }
        }
        appendNullTransactionLogEntryToSetRaftIndexToMinusOne( databaseLayout );
    }

        private void checkRecovered( DatabaseLayout databaseLayout ) throws IOException, RecoveryRequiredException
    {
        try
        {
            assertRecoveryIsNotRequired( fs, pageCache, config, databaseLayout, monitors );
        }
        catch ( RecoveryRequiredException e )
        {
            log.error( e.getMessage() );
            throw e;
        }
    }

    private void moveTransactionLogs( File fromDirectory, File toDirectory ) throws IOException
    {
        if ( fromDirectory.equals( toDirectory ) )
        {
            return;
        }

        fs.mkdirs( toDirectory );
        File[] txLogFiles = fs.listFiles( fromDirectory );

        if ( txLogFiles == null )
        {
            throw new IllegalStateException( "Could not list files in: " + fromDirectory );
        }

        for ( File txLogFile : txLogFiles )
        {
            fs.moveToDirectory( txLogFile, toDirectory );
        }
    }

    private void initializeDatabase( File bootstrapDatabaseDirectory, DatabaseInitializer databaseInitializer, String logsDirectoryName )
    {
        try ( TemporaryDatabase temporaryDatabase = tempDatabaseFactory.startTemporaryDatabase( bootstrapDatabaseDirectory, logsDirectoryName,
                config.get( record_format ) ) )
        {
            if ( databaseInitializer != null )
            {
                databaseInitializer.initialize( temporaryDatabase.graphDatabaseService() );
            }
        }
    }

    private void appendNullTransactionLogEntryToSetRaftIndexToMinusOne( DatabaseLayout databaseLayout ) throws IOException
    {
        ReadOnlyTransactionIdStore readOnlyTransactionIdStore = new ReadOnlyTransactionIdStore( pageCache, databaseLayout );
        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache )
                .withConfig( config )
                .withLastCommittedTransactionIdSupplier( () -> readOnlyTransactionIdStore.getLastClosedTransactionId() - 1 )
                .build();

        long dummyTransactionId;
        try ( Lifespan ignored = new Lifespan( logFiles ) )
        {
            FlushableChannel channel = logFiles.getLogFile().getWriter();
            TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriter( channel ) );

            long lastCommittedTransactionId = readOnlyTransactionIdStore.getLastCommittedTransactionId();
            PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptyList() );
            byte[] txHeaderBytes = LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( -1 );
            tx.setHeader( txHeaderBytes, -1, -1, -1, lastCommittedTransactionId, -1, -1 );

            dummyTransactionId = lastCommittedTransactionId + 1;
            writer.append( tx, dummyTransactionId );
            channel.prepareForFlush().flush();
        }

        File neoStoreFile = databaseLayout.metadataStore();
        MetaDataStore.setRecord( pageCache, neoStoreFile, LAST_TRANSACTION_ID, dummyTransactionId );
    }

    private IdAllocationState deriveIdAllocationState( DatabaseLayout databaseLayout )
    {
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory( fs );

        long[] highIds = new long[]{
                getHighId( factory, NODE, databaseLayout.idNodeStore() ),
                getHighId( factory, RELATIONSHIP, databaseLayout.idRelationshipStore() ),
                getHighId( factory, PROPERTY, databaseLayout.idPropertyStore() ),
                getHighId( factory, STRING_BLOCK, databaseLayout.idPropertyStringStore() ),
                getHighId( factory, ARRAY_BLOCK, databaseLayout.idPropertyArrayStore() ),
                getHighId( factory, PROPERTY_KEY_TOKEN, databaseLayout.idPropertyKeyTokenStore() ),
                getHighId( factory, PROPERTY_KEY_TOKEN_NAME, databaseLayout.idPropertyKeyTokenNamesStore() ),
                getHighId( factory, RELATIONSHIP_TYPE_TOKEN, databaseLayout.idRelationshipTypeTokenStore() ),
                getHighId( factory, RELATIONSHIP_TYPE_TOKEN_NAME, databaseLayout.idRelationshipTypeTokenNamesStore() ),
                getHighId( factory, LABEL_TOKEN, databaseLayout.idLabelTokenStore() ),
                getHighId( factory, LABEL_TOKEN_NAME, databaseLayout.idLabelTokenNamesStore() ),
                getHighId( factory, NEOSTORE_BLOCK, databaseLayout.idMetadataStore() ),
                getHighId( factory, SCHEMA, databaseLayout.idSchemaStore() ),
                getHighId( factory, NODE_LABELS, databaseLayout.idNodeLabelStore() ),
                getHighId( factory, RELATIONSHIP_GROUP, databaseLayout.idRelationshipGroupStore() )};

        return new IdAllocationState( highIds, FIRST_INDEX );
    }

    private static long getHighId( DefaultIdGeneratorFactory factory, IdType idType, File idFile )
    {
        IdGenerator idGenerator = factory.open( idFile, idType, () -> -1L, Long.MAX_VALUE );
        long highId = idGenerator.getHighId();
        idGenerator.close();
        return highId;
    }
}
