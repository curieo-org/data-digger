package org.curieo.rdf;

abstract class RdfTripleBase implements RdfTriple {
	private String subject;
	private String verb;

	protected RdfTripleBase(String s, String v) {
		subject = s;
		verb = v;
	}
	
	@Override
	public void setSubject(String s) {
		subject = s;
	}
	
	@Override
	public void setVerb(String v) {
		verb = v;
	}
	
	@Override
	public String getSubject() {
		return subject;
	}

	@Override
	public String getVerb() {
		return verb;
	}
}
