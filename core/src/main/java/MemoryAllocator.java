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

  private MemoryAllocator() {}

  public static long allocate(int size) {
    return unsafe.allocateMemory(size);
  }

  public static void free(long pointer) {
    unsafe.freeMemory(pointer);
  }
}
