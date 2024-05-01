package org.curieo.model;

import static org.curieo.model.ReferenceType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ReferenceTypeTest {

  @Test
  void referenceTypeOrdinalGuard() {
    // If this test fails you've goofed up the ReferenceType type enum. No worries though, this test
    // caught it :)
    // It is very important that the enum matches the expected integer value, as
    // this is how it is stored in our DB.
    // If you add a new enum value, make sure you also add a check for the correct ordinal.
    validateRefType(Pubmed, 0);
  }

  private void validateRefType(ReferenceType expectedType, int ordinal) {
    // This is function is written this way by design:
    // If a new enum is added this switch will complain at compile time.
    switch (ReferenceType.fromInt(ordinal)) {
      case Pubmed -> assertEquals(Pubmed, expectedType);
    }
  }
}
