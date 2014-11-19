package tbd;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class MemoryAllocator {
  public static final Unsafe unsafe;

  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final long heapSize = 2 * 1024 * 1024;

  private static final long baseAddress = unsafe.allocateMemory(heapSize);

  private static long currentAddress = baseAddress;

  private MemoryAllocator() {}

  public static long allocate(int size) {
    long ret = currentAddress;
    currentAddress += size;

    return ret;
  }
}
