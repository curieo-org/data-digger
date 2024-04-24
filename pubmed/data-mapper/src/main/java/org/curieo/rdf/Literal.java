package org.curieo.rdf;

import static org.curieo.rdf.Constants.XS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;

@Generated
@Data
@AllArgsConstructor
public class Literal {
  public static final Literal TRUE = new Literal(Boolean.TRUE.toString(), null, null);
  public static final Literal FALSE = new Literal(Boolean.FALSE.toString(), null, null);
  private String value;
  private String language;
  private String dataType;

  public Literal(Integer d) {
    this(Integer.toString(d), null, XS + "int");
  }

  public Literal(Double d) {
    this(Double.toString(d), null, XS + "double");
  }

  @Override
  public String toString() {
    return value;
  }
}
