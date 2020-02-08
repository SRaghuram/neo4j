package org.neo4j.storageengine;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.SchemaCache;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.MyMetadataStore;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.*;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokensLoader;
import org.neo4j.util.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;

public class MyStorageEngine implements StorageEngine {

    public MyStore myStore;
    private final TokenHolders tokenHolders;
    SchemaCache schemaCache;
    private final int denseNodeThreshold;
    private final GBPTreeCountsStore countsStore;
    //private WorkSync<NodeLabelUpdateListener,LabelUpdateWork> labelScanStoreSync;
    //private WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync;

    private IndexUpdateListener indexUpdateListener;
    private NodeLabelUpdateListener nodeLabelUpdateListener;
    SchemaRuleAccess schemaRuleAccess;
    ConstraintRuleAccessor constraintSemantics;
    SchemaState schemaState;

    public MyStorageEngine( DatabaseLayout databaseLayout,
                             Config config,
                             PageCache pageCache,
                             FileSystemAbstraction fs,
                             LogProvider logProvider,
                             TokenHolders tokenHolders,
                             SchemaState schemaState,
                             ConstraintRuleAccessor constraintSemantics,
                             IndexConfigCompleter indexConfigCompleter,
                             LockService lockService,
                             Health databaseHealth,
                             IdGeneratorFactory idGeneratorFactory,
                             IdController idController,
                             RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                             boolean createStoreIfNotExists)
    {
        this.tokenHolders = tokenHolders;
        denseNodeThreshold = config.get( GraphDatabaseSettings.dense_node_threshold );
        countsStore = openCountsStore( pageCache, databaseLayout, config, logProvider, recoveryCleanupWorkCollector );
        try {
            myStore = new MyStore(fs, databaseLayout, config, pageCache, idGeneratorFactory, MyStoreVersion.MyStandardFormat, false);
            schemaCache = new SchemaCache( constraintSemantics, indexConfigCompleter );
        } catch (IOException io)
        {
                System.out.println("Error in creating MyStore:"+io.getMessage());
        }
    }

    private GBPTreeCountsStore openCountsStore(PageCache pageCache, DatabaseLayout layout, Config config, LogProvider logProvider,
                                               RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        boolean readOnly = config.get( GraphDatabaseSettings.read_only );
        try
        {
            return new GBPTreeCountsStore(  pageCache, layout.countStore(), recoveryCleanupWorkCollector,
                    new CountsBuilder()
                    {
                        private final Log log = logProvider.getLog(MyMetadataStore.class );

                        @Override
                        public void initialize( CountsAccessor.Updater updater )
                        {
                            log.warn( "Missing counts store, rebuilding it." );
                            new MyCountsComputer( myStore, pageCache, layout ).initialize( updater );
                            log.warn( "Counts store rebuild completed." );
                        }

                        @Override
                        public long lastCommittedTxId()
                        {
                            return myStore.getMetaDataStore().getLastCommittedTransactionId();
                        }
                    }, readOnly, null, GBPTreeCountsStore.NO_MONITOR );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public CommandCreationContext newCommandCreationContext( PageCursorTracer cursorTracer ) {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return new MyCommandCreationContext( myStore );
    }

    @Override
    public void addIndexUpdateListener(IndexUpdateListener listener) {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        Preconditions.checkState( this.indexUpdateListener == null,
                "Only supports a single listener. Tried to add " + listener + ", but " + this.indexUpdateListener + " has already been added" );
        this.indexUpdateListener = listener;
        //this.indexUpdatesSync = new WorkSync<>( listener );
        //this.integrityValidator.setIndexValidator( listener );
    }

    @Override
    public void addNodeLabelUpdateListener(NodeLabelUpdateListener nodeLabelUpdateListener) {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    @Override
    public void createCommands(Collection<StorageCommand> commands,
                               ReadableTransactionState txState, StorageReader storageReader,
                               CommandCreationContext commandCreationContext,
                               ResourceLocker locks,
                               long lastTransactionIdWhenStarted,
                               TxStateVisitor.Decorator additionalTxStateVisitor,
                               PageCursorTracer cursorTracer)  throws KernelException
    {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        if ( txState != null )
        {
            // We can make this cast here because we expected that the storageReader passed in here comes from
            // this storage engine itself, anything else is considered a bug. And we do know the inner workings
            // of the storage statements that we create.
            MyCommandCreationContext creationContext = (MyCommandCreationContext) commandCreationContext;
            MyTransactionState recordState = creationContext.createTransactionRecordState( lastTransactionIdWhenStarted, locks );

            //Visit transaction state and populate these record state objects
            TxStateVisitor txStateVisitor = new MyTransactionToMyStateVisitor( commands, txState, schemaState,
                    schemaRuleAccess, constraintSemantics );
            MyCountsRecordState countsRecordState = new MyCountsRecordState();
            txStateVisitor = additionalTxStateVisitor.apply( txStateVisitor );
            txStateVisitor = new TransactionCountingStateVisitor(
                    txStateVisitor, storageReader, txState, countsRecordState, TRACER_SUPPLIER.get() );
            try ( TxStateVisitor visitor = txStateVisitor )
            {
                txState.accept( visitor );
            }

            // Convert record state into commands
            recordState.extractCommands( commands );
            countsRecordState.extractCommands( commands );
        }
    }

    @Override
    public void apply(CommandsToApply batch, TransactionApplicationMode mode) throws Exception {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    @Override
    public void flushAndForce(IOLimiter limiter, PageCursorTracer cursorTracer ) throws IOException {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    @Override
    public void dumpDiagnostics(DiagnosticsManager diagnosticsManager, Log log) {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    @Override
    public void forceClose() {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
    }


    @Override
    public Collection<StoreFileMetadata> listStorageFiles() {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public StoreId getStoreId() {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return myStore.getMetaDataStore().getStoreId();
    }

    @Override
    public Lifecycle schemaAndTokensLifecycle() {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return new LifecycleAdapter()
        {
            @Override
            public void init()
            {
                tokenHolders.setInitialTokens( allTokens( myStore ), TRACER_SUPPLIER.get() );
                //loadSchemaCache();
            }
        };
        //return null;
    }

    @Override
    public TransactionIdStore transactionIdStore() {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return myStore.getTransactionMetaDataStore();
    }

    @Override
    public LogVersionRepository logVersionRepository() {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return myStore.getTransactionMetaDataStore();
    }

    @Override
    public CountsAccessor countsAccessor() {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return countsStore;
    }

    @Override
    public StorageReader newReader() {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return new MyStorageReader( tokenHolders, myStore, countsStore, schemaCache );
    }

    @Override
    public void init() throws Exception {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    @Override
    public void start() throws Exception {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    @Override
    public void stop() throws Exception {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    @Override
    public void shutdown() throws Exception {
        System.out.println("MyStorageEngine::" + Thread.currentThread().getStackTrace()[1].getMethodName());
    }

    public static TokensLoader allTokens(MyStore myStore )
    {
        return new TokensLoader()
        {
            @Override
            public List<NamedToken> getPropertyKeyTokens( PageCursorTracer cursorTracer )
            {
                return myStore.getPropertyKeyTokens();
            }

            @Override
            public List<NamedToken> getLabelTokens( PageCursorTracer cursorTracer )
            {
                return myStore.getLabelTokens();
            }

            @Override
            public List<NamedToken> getRelationshipTypeTokens( PageCursorTracer cursorTracer )
            {
                return myStore.getRelationshipTypeTokens();
            }
        };
    }
}
