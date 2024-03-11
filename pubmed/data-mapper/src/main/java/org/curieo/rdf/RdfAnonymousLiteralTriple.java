package org.curieo.rdf;

import static org.curieo.rdf.Constants.ANONYMOUS;

/**
 * Rdf Triple
 *
 * @author DoornenbalM
 */
class RdfAnonymousLiteralTriple extends RdfTripleBase implements RdfTriple {
	private Literal object;

	RdfAnonymousLiteralTriple(String s, String v, Literal o) {
		super(s, v);
		object = o;
	}

	@Override
	public void setObject(String o) {
		object = new Literal(o, null, null);
	}
	
	@Override
	public void setLiteralObject(Literal o) {
		object = o;
	}

	@Override
	public String getObject() {
		return object.getValue();
	}

	@Override
	public Literal getLiteralObject() {
		return object;
	}

	@Override
	public boolean isLiteral() {
		return true;
	}
	
	@Override
	public void setUri(String u) {
		throw new UnsupportedOperationException(ANONYMOUS);
	}

	@Override
	public String getUri() {
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
}
