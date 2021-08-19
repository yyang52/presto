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
package com.facebook.presto.common;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.LongArrayBlock;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.lang.Double.longBitsToDouble;

public class PageConvertor
{
    private PageConvertor() {}
    private static final Unsafe _UNSAFE;
    private static final String INT_TYPE_ = "Int";
    private static final String LONG_TYPE_ = "Long";
    private static final String DOUBLE_TYPE_ = "Double";

    static
    {
        sun.misc.Unsafe unsafe;
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (sun.misc.Unsafe) unsafeField.get(null);
        }
        catch (Throwable cause) {
            unsafe = null;
        }
        _UNSAFE = unsafe;
    }

    // Currently, we didn't handle null arrays
    public static long[] toOffHeap(Page page, String[] types)
    {
        int totalBlockCount = page.getChannelCount();
        int positionCount = page.getPositionCount();

        long[] dataBuffers = new long[totalBlockCount];

        int[] sizeForEachBlock = new int[totalBlockCount];
        for (int i = 0; i < totalBlockCount; i++) {
            Block block = page.getBlock(i);
            long bufferSize = block.getSizeInBytes();
            dataBuffers[i] = _UNSAFE.allocateMemory(bufferSize);
            if (block instanceof IntArrayBlock && types[i].equals(INT_TYPE_)) {
                sizeForEachBlock[i] = 4;
                for (int j = 0; j < positionCount; j++) {
                    _UNSAFE.putInt(dataBuffers[i] + j * sizeForEachBlock[i], block.getInt(j));
                }
            }
            else if (block instanceof LongArrayBlock && types[i].equals(LONG_TYPE_)) {
                sizeForEachBlock[i] = 8;
                for (int j = 0; j < positionCount; j++) {
                    _UNSAFE.putLong(dataBuffers[i] + j * sizeForEachBlock[i], block.getLong(j));
                }
            }
            else if (block instanceof LongArrayBlock && types[i].equals(DOUBLE_TYPE_)) {
                sizeForEachBlock[i] = 8;
                for (int j = 0; j < positionCount; j++) {
                    _UNSAFE.putDouble(dataBuffers[i] + j * sizeForEachBlock[i], longBitsToDouble(block.getLong(j)));
                }
            }
            else {
                throw new UnsupportedOperationException("data type " + types[i] + " not supported!!");
            }
        }

        return dataBuffers;
    }

    public static void destroyOffHeapBuffer(long[] dataBuffers)
    {
        for (long dataBuffer : dataBuffers) {
            _UNSAFE.freeMemory(dataBuffer);
        }
    }

    // just for test.
    public static Unsafe getUnsafe()
    {
        return _UNSAFE;
    }

    // TODO: how to represent types? string or others?
    public static Page toPage(long[] dataBuffers, String[] types, int positionCount)
    {
        assert (dataBuffers.length == types.length);
        int blockSize = dataBuffers.length;
        Block[] blocks = new Block[blockSize];
        for (int i = 0; i < blockSize; i++) {
            if (types[i].equals(INT_TYPE_)) {
                BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, positionCount);
                for (int j = 0; j < positionCount; j++) {
                    INTEGER.writeLong(blockBuilder, _UNSAFE.getInt(dataBuffers[i] + Integer.BYTES * j));
                }
                blocks[i] = blockBuilder.build();
            }
            else if (types[i].equals(LONG_TYPE_)) {
                BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, positionCount);
                for (int j = 0; j < positionCount; j++) {
                    BIGINT.writeLong(blockBuilder, _UNSAFE.getLong(dataBuffers[i] + Long.BYTES * j));
                }
                blocks[i] = blockBuilder.build();
            }
            else if (types[i].equals(DOUBLE_TYPE_)) {
                BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, positionCount);
                for (int j = 0; j < positionCount; j++) {
                    DOUBLE.writeDouble(blockBuilder, _UNSAFE.getDouble(dataBuffers[i] + Double.BYTES * j));
                }
                blocks[i] = blockBuilder.build();
            }
            else {
                throw new UnsupportedOperationException("unsupported type: " + types[i]);
            }
        }
        return new Page(positionCount, blocks);
    }
}
