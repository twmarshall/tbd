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

  public static char getChar(long ptr) {
    return unsafe.getChar(ptr);
  }

  public static void putChar(long ptr, char c) {
    unsafe.putChar(ptr, c);
  }

  public static int getInt(long ptr) {
    return unsafe.getInt(ptr);
  }

  public static void putInt(long ptr, int i) {
    unsafe.putInt(ptr, i);
  }

  public static long getPointer(long ptr) {
    return unsafe.getLong(ptr);
  }

  public static void putPointer(long ptr, long p) {
    unsafe.putLong(ptr, p);
  }
}
