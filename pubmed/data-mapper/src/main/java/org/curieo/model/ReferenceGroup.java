package org.curieo.model;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;

@Generated
@Data
@AllArgsConstructor
public class ReferenceGroup {
  String citation;
  Map<String, String> identifiers;
}
