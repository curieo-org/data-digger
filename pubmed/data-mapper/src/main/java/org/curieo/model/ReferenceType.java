package org.curieo.model;

import org.curieo.utils.EnumUtils;

public enum ReferenceType {
  Pubmed;

  public static ReferenceType fromInt(int i) {
    return EnumUtils.get(ReferenceType.class, i);
  }

  public static ReferenceType fromStr(String name) {
    return EnumUtils.search(ReferenceType.class, name);
  }
}
