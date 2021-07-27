/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.orc;

import com.facebook.presto.common.InvalidFunctionArgumentException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcSelectiveRecordReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapd.CiderJNI;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcSelectivePageSource
        implements ConnectorPageSource
{
    private final OrcSelectiveRecordReader recordReader;
    private final OrcDataSource orcDataSource;
    private final OrcAggregatedMemoryContext systemMemoryContext;
    private final FileFormatDataSourceStats stats;

    private boolean closed;

    public OrcSelectivePageSource(
            OrcSelectiveRecordReader recordReader,
            OrcDataSource orcDataSource,
            OrcAggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return recordReader.getReadPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page page = recordReader.getNextPage();
            if (page == null) {
                close();
            }
            return getOutputFromCider(page, recordReader.getSubQuery(), recordReader.getTableColumns());
        }
        catch (InvalidFunctionArgumentException e) {
            closeWithSuppression(e);
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (OrcCorruptionException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_BAD_DATA, e);
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", orcDataSource.getId()), e);
        }
    }

    private Page getOutputFromCider(Page page, String subQuery, String tableColumn)
    {
        int positionCount = page.getPositionCount();
        int count = page.getChannelCount();

        // FIXME columns??
        int resultCount = recordReader.getColumnNum();
        long[] dataBuffers = new long[count];
        long[] dataNulls = new long[count];
        long[] resultBuffers = new long[resultCount];
        long[] resultNulls = new long[resultCount];

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = null;
        try {
            node = mapper.readTree(tableColumn);
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        JsonNode columnArrays = node.get("Columns");
        Iterator<JsonNode> nodes = columnArrays.elements();
        List<String> list = new ArrayList<>();
        while (nodes.hasNext()) {
            JsonNode next = nodes.next();
            Iterator<String> keys = next.fieldNames();
            while (keys.hasNext()) {
                String key = keys.next();
                String value = next.get(key).textValue();
                list.add(value);
            }
        }

        Page output = null;
        long ptr = CiderJNI.getPtr();
        try {
            Unsafe unsafe = getUnsafe();
            for (int i = 0; i < count; i++) {
                if (list.get(i).equals("INT")) {
                    int[] inputBuffer = ((IntArrayBlock) page.getBlock(i)).values;
                    long nullPtr = unsafe.allocateMemory(positionCount);
                    long bufferPtr = unsafe.allocateMemory(positionCount * 4);
                    for (int idx = 0; idx < inputBuffer.length; idx++) {
                        unsafe.putInt(bufferPtr + idx * 4, inputBuffer[idx]);
                    }
                    dataBuffers[i] = bufferPtr;
                    dataNulls[i] = nullPtr;
                }
                else if (list.get(i).equals("LONG")) {
                    long[] inputBuffer = ((LongArrayBlock) page.getBlock(i)).values;
                    long nullPtr = unsafe.allocateMemory(positionCount);
                    long bufferPtr = unsafe.allocateMemory(positionCount * 8);
                    for (int idx = 0; idx < inputBuffer.length; idx++) {
                        unsafe.putLong(bufferPtr + idx * 8, inputBuffer[idx]);
                    }
                    dataBuffers[i] = bufferPtr;
                    dataNulls[i] = nullPtr;
                }
            }

            for (int i = 0; i < resultCount; i++) {
                // FIXME: hardcode now
                String type = "INT";
                int step = 0;
                switch (type) {
                    case "INT":
                        step = 4;
                        break;
                    case "LONG":
                        step = 8;
                        break;
                }
                long nullPtr = unsafe.allocateMemory(positionCount);
                long bufferPtr = unsafe.allocateMemory(positionCount * step);
                resultBuffers[i] = bufferPtr;
                resultNulls[i] = nullPtr;
            }

            int resultSize = CiderJNI.processBlocks(
                    ptr,
                    subQuery,
                    tableColumn,
                    dataBuffers,
                    dataNulls,
                    resultBuffers,
                    resultNulls,
                    positionCount);

            Block[] outputBlocks = new Block[resultCount];
            for (int i = 0; i < resultCount; i++) {
                String type = list.get(i);
                int step = 0;
                switch (type) {
                    case "INT":
                        step = 4;
                        break;
                    case "LONG":
                        step = 8;
                        break;
                }
                long nullPtr = resultNulls[i];
                long resultPtr = resultBuffers[i];
                int index = 0;
                if (type.equals("INT")) {
                    int[] outputBuffer = new int[resultSize];
                    for (int idx = 0; idx < positionCount; idx++) {
                        // FIXME: need to confirm how nullPtr is represented
                        if (unsafe.getByte(nullPtr + idx) == 0x01) {
                            outputBuffer[index++] = unsafe.getInt(resultPtr + idx * step);
                        }
                    }
                    outputBlocks[i] = new IntArrayBlock(resultSize, Optional.empty(), outputBuffer);
                }
                else if (type.equals("LONG")) {
                    long[] outputBuffer = new long[resultSize];
                    for (int idx = 0; idx < positionCount; idx++) {
                        if (unsafe.getByte(nullPtr + idx) == 0x01) {
                            outputBuffer[index++] = unsafe.getLong(resultPtr + idx * step);
                        }
                    }
                    outputBlocks[i] = new LongArrayBlock(resultSize, Optional.empty(), outputBuffer);
                }
            }

            output = new Page(resultSize, outputBlocks);
        }
        catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return output;
    }

    private static Unsafe getUnsafe() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        Unsafe unsafe = (Unsafe) f.get(null);
        return unsafe;
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            stats.addMaxCombinedBytesPerRow(recordReader.getMaxCombinedBytesPerRow());
            recordReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }
}
