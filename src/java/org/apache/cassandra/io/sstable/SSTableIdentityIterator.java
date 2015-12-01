/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Descriptor.Version;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.serializers.MarshalException;

public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, OnDiskAtomIterator
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIdentityIterator.class);

    private final DecoratedKey key;
    private final DataInput in;
    public final long dataSize; // we [still] require this so compaction can tell if it's safe to read the row into memory
    public final ColumnSerializer.Flag flag;

    private final ColumnFamily columnFamily;
    private final int columnCount;

    private final Iterator<OnDiskAtom> atomIterator;
    private final Descriptor.Version dataVersion;

    // Used by lazilyCompactedRow, so that we see the same things when deserializing the first and second time
    private final int expireBefore;

    private final boolean validateColumns;
    private final String filename;

    // Not every SSTableIdentifyIterator is attached to a sstable, so this can be null.
    private final SSTableReader sstable;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param dataSize length of row data
     * @throws IOException
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key, long dataSize)
    {
        this(sstable, file, key, dataSize, false);
    }

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param dataSize length of row data
     * @param checkData if true, do its best to deserialize and check the coherence of row data
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key, long dataSize, boolean checkData)
    {
        this(sstable.metadata, file, file.getPath(), key, dataSize, checkData, sstable, ColumnSerializer.Flag.LOCAL);
    }

    /**
     * Used only by scrubber to solve problems with data written after the END_OF_ROW marker. Iterates atoms for the given dataSize only and does not accept an END_OF_ROW marker.
     */
    public static SSTableIdentityIterator createFragmentIterator(SSTableReader sstable, final RandomAccessReader file, DecoratedKey key, long dataSize, boolean checkData)
    {
        final ColumnSerializer.Flag flag = ColumnSerializer.Flag.LOCAL;
        final int expireBefore = (int) (System.currentTimeMillis() / 1000);
        final Version version = sstable.descriptor.version;
        final long dataEnd = file.getFilePointer() + dataSize;
        return new SSTableIdentityIterator(sstable.metadata, file, file.getPath(), key, dataSize, checkData, sstable, flag, DeletionTime.LIVE, expireBefore, version, Integer.MAX_VALUE,
                                           new AbstractIterator<OnDiskAtom>()
                                                   {
                                                       protected OnDiskAtom computeNext()
                                                       {
                                                           if (file.getFilePointer() >= dataEnd)
                                                               return endOfData();
                                                           try
                                                           {
                                                               return Column.onDiskSerializer().deserializeFromSSTable(file, flag, expireBefore, version);
                                                           }
                                                           catch (IOException e)
                                                           {
                                                               throw new IOError(e);
                                                           }
                                                       }
                                                   });
    }

    // sstable may be null *if* checkData is false
    // If it is null, we assume the data is in the current file format
    private SSTableIdentityIterator(CFMetaData metadata,
                                    DataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    long dataSize,
                                    boolean checkData,
                                    SSTableReader sstable,
                                    ColumnSerializer.Flag flag)
    {
        this(metadata, in, filename, key, dataSize, checkData, sstable, flag,
             (int)(System.currentTimeMillis() / 1000),
             sstable == null ? Descriptor.Version.CURRENT : sstable.descriptor.version);
    }

    private SSTableIdentityIterator(CFMetaData metadata,
                                    DataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    long dataSize,
                                    boolean checkData,
                                    SSTableReader sstable,
                                    ColumnSerializer.Flag flag,
                                    int expireBefore,
                                    Version dataVersion)
    {
        this(metadata, in, filename, key, dataSize, checkData, sstable, flag,
             readDeletionTime(in, sstable, filename),
             expireBefore, dataVersion,
             readColumnCount(in, dataVersion, sstable, filename));
    }

    private SSTableIdentityIterator(CFMetaData metadata,
                                    DataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    long dataSize,
                                    boolean checkData,
                                    SSTableReader sstable,
                                    ColumnSerializer.Flag flag,
                                    DeletionTime deletionTime,
                                    int expireBefore,
                                    Version dataVersion,
                                    int columnCount)
    {
        this(metadata, in, filename, key, dataSize, checkData, sstable, flag,
             deletionTime,
             expireBefore,
             dataVersion,
             columnCount,
             metadata.getOnDiskIterator(in, columnCount, flag, expireBefore, dataVersion));
    }

    private static int readColumnCount(DataInput in, Version dataVersion, SSTableReader sstable, String filename)
    {
        try
        {
            return dataVersion.hasRowSizeAndColumnCount ? in.readInt() : Integer.MAX_VALUE;
        }
        catch (IOException e)
        {
            if (sstable != null)
                sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
    }

    private static DeletionTime readDeletionTime(DataInput in, SSTableReader sstable, String filename)
    {
        try
        {
            return DeletionTime.serializer.deserialize(in);
        }
        catch (IOException e)
        {
            if (sstable != null)
                sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
    }

    private SSTableIdentityIterator(CFMetaData metadata,
                                    DataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    long dataSize,
                                    boolean checkData,
                                    SSTableReader sstable,
                                    ColumnSerializer.Flag flag,
                                    DeletionTime deletion,
                                    int expireBefore,
                                    Version dataVersion,
                                    int columnCount,
                                    Iterator<OnDiskAtom> atomIterator)
    {
        assert !checkData || (sstable != null);
        this.in = in;
        this.filename = filename;
        this.key = key;
        this.dataSize = dataSize;
        this.flag = flag;
        this.validateColumns = checkData;
        this.sstable = sstable;
        this.expireBefore = expireBefore;
        this.columnCount = columnCount;
        this.dataVersion = dataVersion;
        columnFamily = EmptyColumns.factory.create(metadata);
        columnFamily.delete(deletion);
        this.atomIterator = atomIterator;
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return columnFamily;
    }

    public boolean hasNext()
    {
        try
        {
            return atomIterator.hasNext();
        }
        catch (IOError e)
        {
            // catch here b/c atomIterator is an AbstractIterator; hasNext reads the value
            if (e.getCause() instanceof IOException)
            {
                if (sstable != null)
                    sstable.markSuspect();
                throw new CorruptSSTableException((IOException)e.getCause(), filename);
            }
            else
            {
                throw e;
            }
        }
    }

    public OnDiskAtom next()
    {
        try
        {
            OnDiskAtom atom = atomIterator.next();
            if (validateColumns)
                atom.validateFields(columnFamily.metadata());
            return atom;
        }
        catch (MarshalException me)
        {
            throw new CorruptSSTableException(me, filename);
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        // creator is responsible for closing file when finished
    }

    public String getPath()
    {
        // if input is from file, then return that path, otherwise it's from streaming
        if (in instanceof RandomAccessReader)
        {
            RandomAccessReader file = (RandomAccessReader) in;
            return file.getPath();
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public ColumnFamily getColumnFamilyWithColumns(ColumnFamily.Factory containerFactory)
    {
        ColumnFamily cf = columnFamily.cloneMeShallow(containerFactory, false);
        // since we already read column count, just pass that value and continue deserialization
        try
        {
            Iterator<OnDiskAtom> iter = cf.metadata().getOnDiskIterator(in, columnCount, flag, expireBefore, dataVersion);
            while (iter.hasNext())
                cf.addAtom(iter.next());

            if (validateColumns)
            {
                try
                {
                    cf.metadata().validateColumns(cf);
                }
                catch (MarshalException e)
                {
                    throw new RuntimeException("Error validating row " + key, e);
                }
            }
            return cf;
        }
        catch (IOError e)
        {
            // catch here b/c atomIterator is an AbstractIterator; hasNext reads the value
            if (e.getCause() instanceof IOException)
            {
                if (sstable != null)
                    sstable.markSuspect();
                throw new CorruptSSTableException((IOException)e.getCause(), filename);
            }
            else
            {
                throw e;
            }
        }
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }

}
