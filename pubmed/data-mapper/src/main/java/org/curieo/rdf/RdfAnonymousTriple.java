package org.curieo.rdf;

import static org.curieo.rdf.Constants.ANONYMOUS;

/**
 * Rdf Triple
 *
 * @author DoornenbalM
 */
class RdfAnonymousTriple extends RdfTripleBase implements RdfTriple {
	private String object;

	RdfAnonymousTriple(String s, String v, String o) {
		super(s, v);
		object = o;
	}

	public void setObject(String o) {
		object = o;
	}
	
	public void setLiteralObject(Literal o) {
		throw new UnsupportedOperationException("This is a URI triple.");
	}

	public String getObject() {
		return object;
	}

	@Override
	public Literal getLiteralObject() {
		return null;
	}

	/**
	 * Implementations of RdfTriple *must* override hashcode() and equals().
	 * @return
	 */
	@Override
	public int hashCode() {
		return java.util.Objects.hash(getUri(), getSubject(), getVerb(), object);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof RdfTriple) {
			return this.equals((RdfTriple)o);
		}
		return false;
	}
	
	/**
	 * toString() outputs simplified turtle.
	 * @return turtle representation of this triple.
	 */
	@Override
	public String toString() {
		return String.format("%s %s %s .", getSubject(), getVerb(), getObject());
	}

	@Override
	public void setUri(String u) {
		throw new UnsupportedOperationException(ANONYMOUS);
	}

	@Override
	public String getUri() {
		return null;
	}

	@Override
	public boolean isLiteral() {
		return false;
	}
}
