package org.curieo.model.identifiers;

import java.net.URI;
import java.util.Optional;

public interface Identifier {

  String getDefaultField();

  String getNormalized();

  /**
   * Return a URI presentation for the DOI
   *
   * @return an encoded URI representation of the DOI
   */
  Optional<URI> getExternalURI();
}
