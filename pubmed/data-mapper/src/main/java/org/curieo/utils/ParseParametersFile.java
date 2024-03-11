package org.curieo.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ParseParametersFile extends ParseParameters implements AutoCloseable {
	private static final int BUFFER_SIZE = 1024;
	private final char[] buffer = new char[BUFFER_SIZE];
	private int buflen;
	private int posInBuf;
	private final InputStreamReader text;
	private final InputStream is;
	private final String source;
	
	public ParseParametersFile(InputStream s, String source) {
		text = new InputStreamReader(s,UTF_8);
		is = s;
		this.source = source;
	}

	@Override
	public boolean done() {
		refill();
		return posInBuf == -1 || posInBuf >= buflen;
	}
	
	private void refill() {
		if (posInBuf == -1) {
			return;
		}
		if (posInBuf == buflen) {
			try {
				buflen = text.read(buffer, 0, BUFFER_SIZE);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			if (buflen == 0) {
				posInBuf = -1;
				return;
			}
			posInBuf = 0;
		}
	}

	@Override
	public char peek(int ahead) {
		if (posInBuf != -1 && (posInBuf + ahead) < buflen) {
			return buffer[posInBuf + ahead];
		}
		
		throw exception("Cannot look that far ahead");
	}

	public char next() {
		if (done()) {
			throw new RuntimeException("Reading beyond end");
		}
		return buffer[posInBuf];
	}

	public RuntimeException exception(String exception) {
		return new RuntimeException(String.format("Exception %s at position %d, line %d, source %s", exception, pos, line, source));
	}

	@Override
	public void close() throws IOException {
		text.close();
		is.close();
	}

	@Override
	public void uneat(char c) {
		posInBuf--;
		pos--;
		if (c == '\n') {
			line--;
		}
	}

	@Override
	public char eat() {
		char next = next();
		pos++;
		posInBuf++;
		if (next == newLineChar) {
			line++;
			pos = 0;
		}
		return next;
	}
}
