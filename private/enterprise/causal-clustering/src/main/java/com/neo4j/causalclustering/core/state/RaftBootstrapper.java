/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.helper.TemporaryDatabase;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.LongSupplier;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.TransactionMetaDataStore;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.TEMP_BOOTSTRAP_DIRECTORY_NAME;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.internal.id.IdType.ARRAY_BLOCK;
import static org.neo4j.internal.id.IdType.LABEL_TOKEN;
import static org.neo4j.internal.id.IdType.LABEL_TOKEN_NAME;
import static org.neo4j.internal.id.IdType.NEOSTORE_BLOCK;
import static org.neo4j.internal.id.IdType.NODE;
import static org.neo4j.internal.id.IdType.NODE_LABELS;
import static org.neo4j.internal.id.IdType.PROPERTY;
import static org.neo4j.internal.id.IdType.PROPERTY_KEY_TOKEN;
import static org.neo4j.internal.id.IdType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.internal.id.IdType.RELATIONSHIP;
import static org.neo4j.internal.id.IdType.RELATIONSHIP_GROUP;
import static org.neo4j.internal.id.IdType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.internal.id.IdType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.internal.id.IdType.SCHEMA;
import static org.neo4j.internal.id.IdType.STRING_BLOCK;

/**
 * Bootstraps a raft group for a core database. A single instance is chosen as the bootstrapper, by the discovery service.
 * <p>
 * To bootstrap, in this context, means to set the initial state of the cluster, e.g.
 * the initial state of the database, the initial members in the raft group, etc.
 * One could view this as the beginning state of the state machine onto which all the
 * commands from the Raft log get applied to.
 * <p>
 * The input to this process comes from various sources, but for example the set of members which
 * should be part of the initial Raft group comes from the discovery service. The initial state
 * of a store can come from a seed store which has been put in the appropriate location by an operator,
 * or an empty store will simply be created with a random store id.
 * <p>
 * Bootstrapping happens exactly once in the life of a database. However, if the operator uses
 * the unbind tool to rid every member of the cluster state, then bootstrapping will happen
 * yet again.
 */
public class RaftBootstrapper
{
    private static final long FIRST_INDEX = 0L;
    private static final long FIRST_TERM = 0L;

    private final BootstrapContext bootstrapContext;
    private final TemporaryDatabaseFactory tempDatabaseFactory;
    private final DatabaseInitializer databaseInitializer;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Log log;
    private final StorageEngineFactory storageEngineFactory;
    private final Config config;

    public RaftBootstrapper( BootstrapContext bootstrapContext, TemporaryDatabaseFactory tempDatabaseFactory, DatabaseInitializer databaseInitializer,
            PageCache pageCache, FileSystemAbstraction fs, LogProvider logProvider, StorageEngineFactory storageEngineFactory, Config config )
    {
        this.bootstrapContext = bootstrapContext;
        this.tempDatabaseFactory = tempDatabaseFactory;
        this.databaseInitializer = databaseInitializer;
        this.pageCache = pageCache;
        this.fs = fs;
        this.log = logProvider.getLog( getClass() );
        this.storageEngineFactory = storageEngineFactory;
        this.config = config;
    }

    public CoreSnapshot bootstrap( Set<MemberId> members )
    {
        try
        {
            log.info( "Bootstrapping database " + bootstrapContext.databaseId().name() + " for members " + members );
            ensureRecoveredOrThrow( bootstrapContext, config );
            initializeStoreIfNeeded( bootstrapContext );
            appendNullTransactionLogEntryToSetRaftIndexToMinusOne( bootstrapContext );
            CoreSnapshot snapshot = buildCoreSnapshot( members, bootstrapContext );
            log.info( "Bootstrapping of the database " + bootstrapContext.databaseId().name() + " completed " + snapshot );
            return snapshot;
        }
        catch ( Exception e )
        {
            throw new BootstrapException( bootstrapContext.databaseId(), e );
        }
    }

    private void initializeStoreIfNeeded( BootstrapContext bootstrapContext ) throws IOException
    {
        if ( !isStorePresent( bootstrapContext ) )
        {
            File bootstrapRootDir = new File( bootstrapContext.databaseLayout().databaseDirectory(), TEMP_BOOTSTRAP_DIRECTORY_NAME );
            fs.deleteRecursively( bootstrapRootDir ); // make sure temp bootstrap directory does not exist
            try
            {
                String databaseName = bootstrapContext.databaseId().name();
                log.info( "Initializing the store for database " + databaseName + " using a temporary database in " + bootstrapRootDir );
                File bootstrapDbDir = initializeStoreUsingTempDatabase( bootstrapRootDir );

                log.info( "Moving created store files from " + bootstrapDbDir + " to " + bootstrapContext.databaseLayout() );
                bootstrapContext.replaceWith( bootstrapDbDir );
            }
            finally
            {
                fs.deleteRecursively( bootstrapRootDir );
            }
        }
    }

    private File initializeStoreUsingTempDatabase( File bootstrapRootDir )
    {
        try ( TemporaryDatabase tempDatabase = tempDatabaseFactory.startTemporaryDatabase( bootstrapRootDir, config ) )
        {
            databaseInitializer.initialize( tempDatabase.graphDatabaseService() );
            return tempDatabase.defaultDatabaseDirectory();
        }
    }

    private boolean isStorePresent( BootstrapContext bootstrapContext )
    {
        return storageEngineFactory.storageExists( fs, bootstrapContext.databaseLayout(), pageCache );
    }

    private void ensureRecoveredOrThrow( BootstrapContext bootstrapContext, Config config ) throws Exception
    {
        if ( Recovery.isRecoveryRequired( fs, bootstrapContext.databaseLayout(), config ) )
        {
            String message = "Cannot bootstrap database " + bootstrapContext.databaseId().name() + ". " +
                             "Recovery is required. " +
                             "Please ensure that the store being seeded comes from a cleanly shutdown instance of Neo4j or a Neo4j backup";
            log.error( message );
            throw new IllegalStateException( message );
        }
    }

    private CoreSnapshot buildCoreSnapshot( Set<MemberId> members, BootstrapContext bootstrapContext )
    {
        var raftCoreState = new RaftCoreState( new MembershipEntry( FIRST_INDEX, members ) );
        var sessionTrackerState = new GlobalSessionTrackerState();

        var coreSnapshot = new CoreSnapshot( FIRST_INDEX, FIRST_TERM );
        coreSnapshot.add( CoreStateFiles.RAFT_CORE_STATE, raftCoreState );
        coreSnapshot.add( CoreStateFiles.SESSION_TRACKER, sessionTrackerState );

        var idAllocation = deriveIdAllocationState( bootstrapContext.databaseLayout() );
        coreSnapshot.add( CoreStateFiles.ID_ALLOCATION, idAllocation );
        coreSnapshot.add( CoreStateFiles.LOCK_TOKEN, ReplicatedLockTokenState.INITIAL_LOCK_TOKEN );

        return coreSnapshot;
    }

    /**
     * For the purpose of idempotent application from Raft log to the transaction log, every entry in the transaction log
     * carries in its header the corresponding Raft log index. At bootstrap time an empty transaction log entry denoting
     * the beginning of time (Raft log index -1) is created. This is used during recovery by the Raft machinery to pick up
     * where it left off. It is also highly useful for debugging.
     */
    private void appendNullTransactionLogEntryToSetRaftIndexToMinusOne( BootstrapContext bootstrapContext ) throws IOException
    {
        DatabaseLayout layout = bootstrapContext.databaseLayout();
        try ( DatabasePageCache databasePageCache = new DatabasePageCache( pageCache, EmptyVersionContextSupplier.EMPTY ) )
        {
            TransactionIdStore readOnlyTransactionIdStore = storageEngineFactory.readOnlyTransactionIdStore( fs, layout, databasePageCache );
            LogFiles logFiles = LogFilesBuilder
                    .activeFilesBuilder( layout, fs, databasePageCache )
                    .withConfig( config )
                    .withLastCommittedTransactionIdSupplier( () -> readOnlyTransactionIdStore.getLastClosedTransactionId() - 1 )
                    .build();

            long dummyTransactionId;
            LogPositionMarker logPositionMarker = new LogPositionMarker();
            try ( Lifespan ignored = new Lifespan( logFiles ) )
            {
                FlushablePositionAwareChannel channel = logFiles.getLogFile().getWriter();
                TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriter( channel ) );

                long lastCommittedTransactionId = readOnlyTransactionIdStore.getLastCommittedTransactionId();
                PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptyList() );
                byte[] txHeaderBytes = LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( -1 );
                tx.setHeader( txHeaderBytes, -1, -1, -1, lastCommittedTransactionId, -1, -1 );

                dummyTransactionId = lastCommittedTransactionId + 1;
                channel.getCurrentPosition( logPositionMarker );
                writer.append( tx, dummyTransactionId );
                channel.prepareForFlush().flush();
            }

            try ( TransactionMetaDataStore transactionMetaDataStore = storageEngineFactory.transactionMetaDataStore( fs, layout, config, databasePageCache ) )
            {
                transactionMetaDataStore.setLastCommittedAndClosedTransactionId( dummyTransactionId, 0, currentTimeMillis(),
                        logPositionMarker.getByteOffset(), logPositionMarker.getLogVersion() );
            }
        }
    }

    /**
     * In a cluster the ID allocations are based on consensus (rafted). This looks at the store (seed or empty) and
     * figures out what the initial bootstrapped state is.
     */
    private IdAllocationState deriveIdAllocationState( DatabaseLayout layout )
    {
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory( fs );

        long[] highIds = new long[]{
                getHighId( factory, NODE, layout.idNodeStore() ),
                getHighId( factory, RELATIONSHIP, layout.idRelationshipStore() ),
                getHighId( factory, PROPERTY, layout.idPropertyStore() ),
                getHighId( factory, STRING_BLOCK, layout.idPropertyStringStore() ),
                getHighId( factory, ARRAY_BLOCK, layout.idPropertyArrayStore() ),
                getHighId( factory, PROPERTY_KEY_TOKEN, layout.idPropertyKeyTokenStore() ),
                getHighId( factory, PROPERTY_KEY_TOKEN_NAME, layout.idPropertyKeyTokenNamesStore() ),
                getHighId( factory, RELATIONSHIP_TYPE_TOKEN, layout.idRelationshipTypeTokenStore() ),
                getHighId( factory, RELATIONSHIP_TYPE_TOKEN_NAME, layout.idRelationshipTypeTokenNamesStore() ),
                getHighId( factory, LABEL_TOKEN, layout.idLabelTokenStore() ),
                getHighId( factory, LABEL_TOKEN_NAME, layout.idLabelTokenNamesStore() ),
                getHighId( factory, NEOSTORE_BLOCK, layout.idMetadataStore() ),
                getHighId( factory, SCHEMA, layout.idSchemaStore() ),
                getHighId( factory, NODE_LABELS, layout.idNodeLabelStore() ),
                getHighId( factory, RELATIONSHIP_GROUP, layout.idRelationshipGroupStore() )};

        return new IdAllocationState( highIds, FIRST_INDEX );
    }

    private static long getHighId( DefaultIdGeneratorFactory factory, IdType idType, File idFile )
    {
        LongSupplier throwingHighIdSupplier = throwIfIdFileDoesNotExist( idType, idFile );
        try ( IdGenerator idGenerator = factory.open( idFile, idType, throwingHighIdSupplier, Long.MAX_VALUE ) )
        {
            return idGenerator.getHighId();
        }
    }

    /**
     * ID files should always be available when this bootstrapper tries to read the high ID.
     * This method returns a high ID supplier that always throws.
     * The high ID supplier is only used by the {@link DefaultIdGeneratorFactory} when ID file does not exist.
     *
     * @param type the type of ID.
     * @param file the ID file.
     * @return supplier that always throws.
     */
    private static LongSupplier throwIfIdFileDoesNotExist( IdType type, File file )
    {
        return () ->
        {
            throw new IllegalStateException( "Unable to read high ID of type " + type + " from " + file );
        };
    }
}
