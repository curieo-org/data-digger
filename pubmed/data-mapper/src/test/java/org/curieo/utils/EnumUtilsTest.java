package org.curieo.utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EnumUtilsTest {

  enum TestCase {
    A,
    a,
    b,
    B
  }

  @Test
  void testEnumSearch() {
    assertEquals(TestCase.A, EnumUtils.search(TestCase.class, "A"));
    assertEquals(TestCase.b, EnumUtils.search(TestCase.class, "B"));
  }

  @Test
  void testGet() {
    assertEquals(TestCase.A, EnumUtils.get(TestCase.class, 0));
    assertEquals(TestCase.B, EnumUtils.get(TestCase.class, 3));
  }
}
