package com.baidu.storm.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

public final class BaseCounter<T> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final Map<T, ArrayList<Long>> objToCounts = new HashMap<T, ArrayList<Long>>();
	
	public BaseCounter() {
	}
	
	
	//add records to hashmap
	public void incrementCount(T obj, Long tn) {
		ArrayList<Long> counts = objToCounts.get(obj);
		if(null == counts) {
			counts = new ArrayList<Long>();
			objToCounts.put(obj, counts);
		}
		counts.add(tn);
	}
	
	//get all counts of every objects and then restored in hashmap
	public Map<T, Long> getCounts() {
		Map<T, Long> result = new HashMap<T, Long>();
		for (T obj : objToCounts.keySet()) {
			result.put(obj, computeTotalCount(obj));
		}
		return result;
	}

	private long computeTotalCount(T obj) {
		// TODO Auto-generated method stub
		ArrayList<Long> curr = objToCounts.get(obj);
		long total = 0;
		for (Long l : curr) {
			total += l.longValue();
		}
		return total;
	}
	
	
/*	//get size of every objects and then restored in hashmap
	public Map<T, Integer> getArraySize() {
		Map<T, Integer> size = new HashMap<T, Integer>();
		for (T obj : objToCounts.keySet()) {
			size.put(obj, computeTotalSize(obj));
		}
		return size;
	}*/

	public int computeObjectSize(T obj) {
		// TODO Auto-generated method stub
		ArrayList<Long> list = objToCounts.get(obj);
		return list.size();
	}
	
	public void wipeArrayList() {
		for (T obj : objToCounts.keySet()) {
			resetCountToZero(obj);
		}
	}

	//reset all the counters to zero
	private void resetCountToZero(T obj) {
		// TODO Auto-generated method stub
		ArrayList<Long> counts = objToCounts.get(obj);
		counts.clear();
	}
	
	//reset all the objects
	public void wipeObjects() {
		Set<T> objToBeRemoved = new HashSet<T>();
		for (T obj : objToCounts.keySet()) {
			objToBeRemoved.add(obj);
		}   
		for (T obj : objToBeRemoved) {
			objToCounts.remove(obj);
		}
	} 
}
