package org.curieo.utils;

public class EnumUtils {
  /**
   * Gets enum value from ordinal, if present.
   * @param enumClass
   * @param i
   * @return
   * @param <T>
   */
  public static <T extends Enum<?>> T get(Class<T> enumClass, int i) {
    return enumClass.getEnumConstants()[i];
  }

  /**
   * Finds enum value matching name. Works like valueOf() except case-insensitive.
   * Note:
   *   If there are enum values that have the same name when not considering casing it will return the first.
   * @param enumClass
   * @param name
   * @return
   * @param <T>
   */
  public static <T extends Enum<?>> T search(Class<T> enumClass, String name) {
    for (T each : enumClass.getEnumConstants()) {
      if (each.name().compareToIgnoreCase(name) == 0) {
        return each;
      }
    }
    return null;
  }
}
