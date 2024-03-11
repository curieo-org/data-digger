package org.curieo.data;

import java.util.Iterator;
import java.util.stream.IntStream;

public class Range implements Iterable<Integer> {
	int start;
	int end;
	
	public Range(int s, int e) {
		if (e < s) {
			throw new IllegalArgumentException("End must be larger or equal to start");
		}
		start = s;
		end = e;
	}

	public int getStart() {
		return start;
	}
	public int getEnd() {
		return end;
	}
	public void setStart(int s) {
		start = s;
	}
	public void setEnd(int e) {
		end = e;
	}
	public int getLength() {
		return end-start;
	}

	public String getString(String s) {
		return s.substring(start, end);
	}

	@Override
	public Iterator<Integer> iterator() {
		return IntStream.range(start, end).iterator();
	}

	public IntStream stream() {
		return IntStream.range(start, end);
	}

	public Range trim(String text) {
		while (start < end && Character.isWhitespace(text.charAt(end))) {
			start++;
		}
		while (start < end && Character.isWhitespace(text.charAt(end-1))) {
			end--;
		}
		return this;
	}
}
