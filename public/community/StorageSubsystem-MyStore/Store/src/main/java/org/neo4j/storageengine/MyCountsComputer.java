package org.neo4j.storageengine;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MyStore;

public class MyCountsComputer implements CountsBuilder {

    MyStore myStore;
    PageCache pageCache;
    DatabaseLayout databaseLayout;
    public MyCountsComputer(MyStore store, PageCache pageCache, DatabaseLayout databaseLayout )
    {
        myStore = store;
        this.pageCache = pageCache;
        this.databaseLayout = databaseLayout;
    }
    @Override
    public void initialize(CountsAccessor.Updater updater) {

    }

    @Override
    public long lastCommittedTxId() {
        return 0;
    }
}
