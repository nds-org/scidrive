/*******************************************************************************
 * Copyright 2013 Johns Hopkins University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package edu.jhu.pha.vospace.process.tika;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class LineAnalyser {
	int numLines;
	private Map<Character, List<Integer>> characterFrequency;
	private Map<Character, Map<Integer,Integer>> lineFrequency;
	
	public LineAnalyser (Set<Character> delimiters) {
		characterFrequency = new HashMap<Character, List<Integer>>();
		lineFrequency = new HashMap<Character, Map<Integer,Integer>>();
		Iterator<Character> i = delimiters.iterator();
		while (i.hasNext()) {
			char ch = i.next();
			characterFrequency.put(ch, new ArrayList<Integer>());
			lineFrequency.put(ch, new HashMap<Integer,Integer>());
		}
		numLines = 0;
	}
	
	public void addLine(String s) {
		numLines++;

		Iterator<Character> i = characterFrequency.keySet().iterator();
		while (i.hasNext()) {
			char ch = i.next();
			List<Integer> list = characterFrequency.get(ch);
			list.add(getCharacterFrequency(s,ch));
			
		}
	}
	
	public int getCharacterFrequency(String s, char ch) {
		int fromIndex = 0, result = 0;
		int frequency = 0;
		while (result != -1) {
			result = s.indexOf(ch, fromIndex);
			if (result != -1) {
				frequency++;
				fromIndex = result+1;
			}
		}
		return frequency;
	}
	
	public void analyse() {
		Iterator<Character> i = characterFrequency.keySet().iterator();
		while (i.hasNext()) {
			char ch = i.next();
			List<Integer> list = characterFrequency.get(ch);
			Map<Integer,Integer> map = lineFrequency.get(ch);
			for (int n: list) {
				if (n!=0) {
					if (map.containsKey(n)) {
						map.put(n,map.get(n)+1);
					}
					else {
						map.put(n,1);
					}
				}
			}
		}
	}
	
	public int getDelimiterRating(char c) {
		Map<Integer,Integer> map = lineFrequency.get(c);
		Iterator<Entry<Integer,Integer>> i = map.entrySet().iterator();
		int rating = 0;
		while (i.hasNext()) {
			Entry<Integer,Integer> entry = i.next();
			int value = entry.getValue(); 
			if (value > rating) {
				rating = value;
			}
		}
		return rating;
	}
	
	public int getDelimiterCount(char c) {
		Map<Integer,Integer> map = lineFrequency.get(c);
		Iterator<Entry<Integer,Integer>> i = map.entrySet().iterator();
		int rating = 0;
		int count = 0;
		while (i.hasNext()) {
			Entry<Integer,Integer> entry = i.next();
			int value = entry.getValue(); 
			if (value > rating) {
				rating = value;
				count = entry.getKey();
			}
		}
		return count;
	}
	
	public Set<Character> getBestDelimiters() {
		Set<Character> result = new HashSet<Character>();
		int bestRating = 0;
		Iterator<Character> i = characterFrequency.keySet().iterator();
		while (i.hasNext()) {
			char ch = i.next();
			int rating = getDelimiterRating(ch);
			if (rating > bestRating) {
				result.clear();
				result.add(ch);
				bestRating = rating;
			}
			else if (rating == bestRating && rating != 0) {
				result.add(ch);
			}
		}
		return result;
	}
	
	public int getNumLines() {
		return numLines;
	}
	
	public int getNumHeaderLines(char delimiter) {
		int count = getDelimiterCount(delimiter);
		int currentLine = 0;
		while (currentLine < numLines && characterFrequency.get(delimiter).get(currentLine) != count) {
			currentLine++;
		}
		
		return currentLine;
	}
	
	public int getNumDataLines(char delimiter) {
		int numHeaderLines = getNumHeaderLines(delimiter);
		
		int count = getDelimiterCount(delimiter);
		int currentLine = numHeaderLines;
		while (currentLine < numLines && characterFrequency.get(delimiter).get(currentLine) == count) {
			currentLine++;
		}
		
		return currentLine - numHeaderLines;
	}
	
	public boolean isTableLine(String s, char ch) {
		if (getDelimiterCount(ch) == getCharacterFrequency(s,ch)) return true;
		else return false;
	}
}



