package tbd;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class MemoryAllocator {
  private static final Unsafe unsafe;

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

  public static void free(long ptr) {
    unsafe.freeMemory(ptr);
  }

  public static byte getByte(long ptr) {
    return unsafe.getByte(ptr);
  }

  public static void putByte(long ptr, byte b) {
    unsafe.putByte(ptr, b);
  }

  public static int getInt(long ptr) {
    return unsafe.getInt(ptr);
  }

  public static void putInt(long ptr, int i) {
    unsafe.putInt(ptr, i);
  }

  public static long getModId(long ptr) {
    return unsafe.getLong(ptr);
  }

  public static void putModId(long ptr, long modId) {
    unsafe.putLong(ptr, modId);
  }

  public static long getPointer(long ptr) {
    return unsafe.getLong(ptr);
  }

  public static void putPointer(long ptr, long pointer) {
    unsafe.putLong(ptr, pointer);
  }
}
