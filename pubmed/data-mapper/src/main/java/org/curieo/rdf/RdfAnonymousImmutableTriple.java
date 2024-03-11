package org.curieo.rdf;

import static org.curieo.rdf.Constants.IMMUTABLE;

/**
 * Rdf Triple
 *
 * @author DoornenbalM
 */
public class RdfAnonymousImmutableTriple implements RdfTriple {
	private final String subject;
	private final ImmutableVerbObject rvo;

	public RdfAnonymousImmutableTriple(String s, ImmutableVerbObject rvo) {
		this.subject = s;
		this.rvo = rvo;
	}

	@Override
	public void setSubject(String s) {
		throw new UnsupportedOperationException(IMMUTABLE);
	}

	@Override
	public void setVerb(String v) {
		throw new UnsupportedOperationException(IMMUTABLE);
	}

	@Override
	public String getSubject() {
		return subject;
	}

	@Override
	public String getVerb() {
		return rvo.getVerb();
	}

	public void setObject(String o) {
		throw new UnsupportedOperationException(IMMUTABLE);
	}

	public void setLiteralObject(Literal o) {
		throw new UnsupportedOperationException(IMMUTABLE);
	}

	public String getObject() {
		return rvo.getObject();
	}

	@Override
	public boolean isLiteral() {
		return rvo.isLiteral();
	}

	@Override
	public Literal getLiteralObject() {
		return rvo.getLiteralObject();
	}

	/**
	 * Implementations of RdfTriple *must* override hashcode() and equals().
	 * @return
	 */
	@Override
	public int hashCode() {
		return java.util.Objects.hash(getUri(), getSubject(), getVerb(), getObject());
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
		return String.format("%s %s %s .", getSubject(), getVerb(), isLiteral() ? getLiteralObject() : getObject() );
	}

	@Override
	public void setUri(String u) {
		throw new UnsupportedOperationException("This is an anonymous triple.");
	}

	@Override
	public String getUri() {
		return null;
	}
}
