package org.curieo.data;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import lombok.Generated;
import lombok.Value;

public class BracketedRange extends Range {
	final List<BracketedRange> embedded = new ArrayList<>();
	public BracketedRange(int s, int e) {
		super(s, e);
	}
	
	public List<BracketedRange> getEmbedded() {
		return embedded;
	}

	public String getEnclosed(String s) {
		return s.substring(start+1, end-1).trim();
	}
	
	public static List<BracketedRange> extractedBracketedRanges(String s) {
		List<BracketedRange> retval = new ArrayList<>(); 
		Deque<Opener> openers = new ArrayDeque<>();
		for (int p = 0; p < s.length(); p++) {
			switch (s.charAt(p)) {
			case '(':
				openers.push(new Opener(')', new BracketedRange(p, p)));
				break;
			case '[':
				openers.push(new Opener(']', new BracketedRange(p, p)));
				break;
			case '<':
				openers.push(new Opener('>', new BracketedRange(p, p)));
				break;
			default:
				if (!openers.isEmpty() && s.charAt(p) == openers.peek().getCloser()) {
					Opener range = openers.pop();
					range.range.setEnd(p+1);
					if (openers.isEmpty()) {
						retval.add(range.range);
					}
					else {
						openers.peek().getRange().getEmbedded().add(range.range);
					}
				}
			}
		}
		
		// add remaining (only completed) ranges
		if (!openers.isEmpty()) {
			retval.addAll(openers.pop().getRange().getEmbedded());
		}
		return retval;
	}

	@Generated @Value
	private static class Opener {
		char closer;
		BracketedRange range;
	}
}
