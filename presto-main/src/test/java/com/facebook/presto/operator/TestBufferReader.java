package com.facebook.presto.operator;

import org.testng.annotations.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import static org.testng.Assert.assertEquals;

public class TestBufferReader {

    @Test
    public static void testBufferRead(int positionCount) throws NoSuchFieldException, IllegalAccessException {
        Unsafe unsafe = getUnsafe();
        long nullPtr = unsafe.allocateMemory(positionCount);
        long bufferPtr = unsafe.allocateMemory(positionCount * 4);
        // result buffer for test
        int resultSize = 8;
        int[] results = new int[]{4, Integer.MIN_VALUE, 5, 8, 10, Integer.MIN_VALUE, 11, 12, 17, 18};
        for (int idx = 0; idx < results.length; idx++) {
            unsafe.putInt(bufferPtr + idx * 4, results[idx]);
        }
        // a as null
        Byte a = 0x01;
        Byte b = 0x00 ;
        for (int idx = 0; idx < results.length; idx++) {
            if (results[idx] == Integer.MIN_VALUE) {
                unsafe.putByte(nullPtr + idx, a);
            }
            else {
                unsafe.putByte(nullPtr + idx, b);
            }
        }
        // read output
        int[] outputBuffer = new int[resultSize];
        int i = 0;
        for (int idx = 0; idx < positionCount; idx++) {
            if (unsafe.getByte(nullPtr + idx) != a) {
                outputBuffer[i++] = unsafe.getInt(bufferPtr + idx * 4);
            }
        }
        // check equal
        int[] output = new int[]{4, 5, 8, 10, 11, 12, 17, 18};
        for (int j = 0; j < resultSize; j++) {
            assertEquals(outputBuffer[j], output[j]);
        }
    }

    private static Unsafe getUnsafe() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException
    {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        Unsafe unsafe = (Unsafe) f.get(null);
        return unsafe;
    }

    public static void main(String [] args) throws NoSuchFieldException, IllegalAccessException {
        testBufferRead(10);
    }
}

