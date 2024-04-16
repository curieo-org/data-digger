package org.curieo.sources;

import java.util.List;
import lombok.Builder;
import lombok.Generated;
import lombok.Value;
import org.curieo.model.Metadata;

@Generated
@Value
@Builder
public class Source {
  String type;
  List<Metadata> identifiers;
}
