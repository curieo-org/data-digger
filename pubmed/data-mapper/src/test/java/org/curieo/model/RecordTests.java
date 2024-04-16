package org.curieo.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Calendar;
import org.junit.jupiter.api.Test;

class RecordTests {

  @Test
  void testFormatting() {
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.MONTH, 0);
    calendar.set(Calendar.YEAR, 2000);
    calendar.set(Calendar.DAY_OF_MONTH, 1);
    assertEquals("2000-01-01", Record.formatDate(calendar.getTime()));
  }
}
