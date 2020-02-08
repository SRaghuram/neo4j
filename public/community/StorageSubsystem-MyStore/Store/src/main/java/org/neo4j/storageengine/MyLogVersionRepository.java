package org.neo4j.storageengine;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.LogVersionRepository;

import java.io.File;
import java.io.IOException;

public class MyLogVersionRepository implements LogVersionRepository {
    private long logVersion = 0;
    private volatile boolean incrementVersionCalled;

    public MyLogVersionRepository(PageCache pageCache, DatabaseLayout databaseLayout ) throws IOException
    {
        System.out.println("MyLogVersionRepository::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        File neoStore = databaseLayout.metadataStore();
        this.logVersion = 0;//readLogVersion( pageCache, neoStore );
    }

    @Override
    public long getCurrentLogVersion()
    {
        System.out.println("MyLogVersionRepository::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        // We can expect a call to this during shutting down, if we have a LogFile using us.
        // So it's sort of OK.
        if ( incrementVersionCalled )
        {
            throw new IllegalStateException( "Read-only log version repository has observed a call to " +
                    "incrementVersion, which indicates that it's been shut down" );
        }
        return logVersion;
    }

    @Override
    public void setCurrentLogVersion( long version, PageCursorTracer cursorTracer )
    {
        System.out.println("MyLogVersionRepository::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        //throw new UnsupportedOperationException( "Can't set current log version in read only version repository." );
    }

    @Override
    public long incrementAndGetVersion()
    {
        System.out.println("MyLogVersionRepository::" + Thread.currentThread().getStackTrace()[1].getMethodName());// We can expect a call to this during shutting down, if we have a LogFile using us.
        // So it's sort of OK.
        if ( incrementVersionCalled )
        {
            throw new IllegalStateException( "Read-only log version repository only allows " +
                    "to call incrementVersion once, during shutdown" );
        }
        //incrementVersionCalled = true;
        return logVersion;
    }

    /*private static long readLogVersion( PageCache pageCache, File neoStore ) throws IOException
    {
        try
        {
            return logVersion;//MetaDataStore.getRecord( pageCache, neoStore, MetaDataStore.Position.LOG_VERSION );
        }
        catch ( NoSuchFileException ignore )
        {
            return 0;
        }
    }*/

}
