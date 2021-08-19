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
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.lang.Double.longBitsToDouble;
import static org.testng.Assert.assertEquals;

public class TestPageConvertor
{
    @Test
    public void testLong()
    {
        int entries = 10;
        String[] types = {"Long", "Long", "Long"};
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block, block, block);
        long[] ret = PageConvertor.toOffHeap(page, types);
        assertEquals(ret.length, 3);
        long dataBuffer0 = ret[0];
        for (int i = 0; i < entries; i++) {
            assertEquals(PageConvertor.getUnsafe().getLong(dataBuffer0 + i * Long.BYTES), (long) i);
        }
        PageConvertor.destroyOffHeapBuffer(ret);
    }

    @Test
    public void testDouble()
    {
        int entries = 10;
        String[] types = {"Double", "Double", "Double"};
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            DOUBLE.writeDouble(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block, block, block);
        long[] ret = PageConvertor.toOffHeap(page, types);
        assertEquals(ret.length, 3);
        long dataBuffer0 = ret[0];
        for (int i = 0; i < entries; i++) {
            assertEquals(PageConvertor.getUnsafe().getDouble(dataBuffer0 + i * Double.BYTES), (double) i);
        }
        PageConvertor.destroyOffHeapBuffer(ret);
    }

    @Test
    public void testInt()
    {
        int entries = 10;
        String[] types = {"Int", "Int", "Int"};
        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            INTEGER.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block, block, block);
        long[] ret = PageConvertor.toOffHeap(page, types);
        assertEquals(ret.length, 3);
        long dataBuffer0 = ret[0];
        for (int i = 0; i < entries; i++) {
            assertEquals(PageConvertor.getUnsafe().getInt(dataBuffer0 + i * Integer.BYTES), i);
        }
        PageConvertor.destroyOffHeapBuffer(ret);
    }

    @Test
    public void testToPage()
    {
        int entries = 10;
        String[] types = {"Int", "Long", "Double"};
        long[] dataBuffers = new long[3];

        // int
        dataBuffers[0] = PageConvertor.getUnsafe().allocateMemory(entries * Integer.BYTES);
        for (int i = 0; i < entries; i++) {
            PageConvertor.getUnsafe().putInt(dataBuffers[0] + i * Integer.BYTES, i * 10);
        }

        // Long
        dataBuffers[1] = PageConvertor.getUnsafe().allocateMemory(entries * Long.BYTES);
        for (int i = 0; i < entries; i++) {
            PageConvertor.getUnsafe().putLong(dataBuffers[1] + i * Long.BYTES, i * 2);
        }

        // Double
        dataBuffers[2] = PageConvertor.getUnsafe().allocateMemory(entries * Double.BYTES);
        for (int i = 0; i < entries; i++) {
            PageConvertor.getUnsafe().putDouble(dataBuffers[2] + i * Double.BYTES, i * 2.5);
        }

        Page page = PageConvertor.toPage(dataBuffers, types, entries);

        assertEquals(page.getChannelCount(), 3);
        assertEquals(page.getPositionCount(), 10);
        for (int i = 0; i < entries; i++) {
            assertEquals(page.getBlock(0).getInt(i), i * 10);
            assertEquals(page.getBlock(1).getLong(i), i * 2);
            assertEquals(longBitsToDouble(page.getBlock(2).getLong(i)), i * 2.5);
        }
    }
}
