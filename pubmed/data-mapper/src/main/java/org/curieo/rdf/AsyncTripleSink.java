package org.curieo.rdf;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * An asynchronous triple store puts the triple store in another thread, 
 * allowing you to generate the triples in another (main) thread.
 * When done, call .get() to complete the storage of triples.
 * You can put any other triple store in the back
 * 
 * @author doornenbalm
 */
public class AsyncTripleSink<T extends TripleStore> implements TripleStore, Supplier<T>  {
	private final T store;
	private final BlockingQueue<Message> queue;
	private final Worker worker;
	
	public AsyncTripleSink(T store) {
		this.store = store;
		this.queue = new ArrayBlockingQueue<>(100);
		worker = new Worker();
		worker.start();
	}
	
	/**
	 * Implementation according to interface, but always returning null
	 * You cannot use the triple right away as the storage is asynchronous
	 * @param uri can be null
	 * @param subject cannot be null
	 * @param verb cannot be null
	 * @param object cannot be null
	 * @return null
	 */
	@Override
	public RdfTriple assertTriple(String uri, String subject, String verb, String object) {
		try {
			queue.put(new Message(uri, subject, verb, object, false));
		} catch (InterruptedException e) {
			worker.interrupt();
			Thread.currentThread().interrupt();
		}
		return null;
	}

	/**
	 * Implementation according to interface, but always returning null.
	 * You cannot use the triple right away as the storage is asynchronous
	 * @param uri can be null
	 * @param subject cannot be null
	 * @param verb cannot be null
	 * @param object cannot be null
	 * @return null
	 */
	@Override
	public RdfTriple assertTriple(String uri, String subject, String verb, Literal object) {
		try {
			queue.put(new Message(uri, subject, verb, object));
		} catch (InterruptedException e) {
			worker.interrupt();
			Thread.currentThread().interrupt();
		}
		return null;
	}
	
	@Override
	public RdfTriple accept(RdfTriple triple) {
		try {
			queue.put(new Message(triple));
		} catch (InterruptedException e) {
			worker.interrupt();
			Thread.currentThread().interrupt();
		}
		return null;
	}

	@Override
	public T get() {
		try {
			queue.put(DONE);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		try {
			worker.join();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return store;
	}

	static final Message DONE = new Message();
	private static class Message {
		final String subject;
		final String verb;
		final String object;
		final String uri;
		final Literal literal;
		final RdfTriple triple;
		final boolean done;
		final boolean andName;
		Message(String u, String s, String v, String o, boolean andName) {
			if (andName && u == null) {
				throw new IllegalArgumentException("u");
			}
			this.uri = u;
			this.subject = s;
			this.verb = v;
			this.object = o;
			this.literal = null;
			this.andName = andName;
			done = false;
			triple = null;
		}
		Message(RdfTriple triple) {
			this.uri = null;
			this.subject = null;
			this.verb = null;
			this.object = null;
			this.literal = null;
			this.andName = false;
			this.triple = triple;
			done = false;
		}
		Message(String u, String s, String v, Literal o) {
			this.uri = u;
			this.subject = s;
			this.verb = v;
			this.object = null;
			this.literal = o;
			this.andName = false;
			triple = null;
			done = false;
		}
		Message() {
			done = true;
			this.uri = null;
			this.subject = null;
			this.object = null;
			this.literal = null;
			this.andName = false;
			this.verb = null;
			triple = null;
		}
	}
	
	class Worker extends Thread {
		@Override
		public void run() {
			Message message;
			do {
				try {
					message = queue.take();
					if (!message.done) {
						if (message.literal == null) {
							if (message.triple != null) {
								store.accept(message.triple);
							}
							else if (message.andName) {
								store.assertAndNameTriple(message.uri, message.subject, message.verb, message.object);
							}
							else {
								store.assertTriple(message.uri, message.subject, message.verb, message.object);
							}
						}
						else {
							store.assertTriple(message.uri, message.subject, message.verb, message.literal);
						}
					}
				} catch (InterruptedException e) {
					this.interrupt();
					message = DONE;
				}
			} while (!message.done);
		}
	}

	@Override
	public RdfTriple assertAndNameTriple(String uri, String subject, String verb, String object) {
		try {
			queue.put(new Message(uri, subject, verb, object, true));
		} catch (InterruptedException e) {
			worker.interrupt();
			Thread.currentThread().interrupt();
		}
		return null;
	}
}
