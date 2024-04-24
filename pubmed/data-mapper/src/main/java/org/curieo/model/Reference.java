package org.curieo.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;

@Generated
@Data
@AllArgsConstructor
public class Reference {
  String citation;
  List<Metadata> identifiers;
}
