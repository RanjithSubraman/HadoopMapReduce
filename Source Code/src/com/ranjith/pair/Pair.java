package com.ranjith.pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	private Text firstElement;
	private Text secondElement;

	public Pair() {
		firstElement = new Text();
		secondElement = new Text();
	}

	public Pair(String firstStringElement, String secondStringElement) {
		firstElement = new Text(firstStringElement);
		secondElement = new Text(secondStringElement);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((firstElement == null) ? 0 : firstElement.hashCode());
		result = prime * result
				+ ((secondElement == null) ? 0 : secondElement.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (firstElement == null) {
			if (other.firstElement != null)
				return false;
		} else if (!firstElement.equals(other.firstElement))
			return false;
		if (secondElement == null) {
			if (other.secondElement != null)
				return false;
		} else if (!secondElement.equals(other.secondElement))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		firstElement.readFields(input);
		secondElement.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		firstElement.write(output);
		secondElement.write(output);
	}

	@Override
	public int compareTo(Pair p) {
		int compare = firstElement.compareTo(p.firstElement);
		if (compare != 0)
			return compare;
		if (secondElement.toString() == "*")
			return -1;
		else if (p.secondElement.toString() == "*")
			return 1;
		else
			return secondElement.compareTo(p.secondElement);
	}

	@Override
	public String toString() {
		return "(" + firstElement + ", " + secondElement + ")";
	}

	public Text getFirstElement() {
		return firstElement;
	}

	public Text getSecondElement() {
		return secondElement;
	}

}
