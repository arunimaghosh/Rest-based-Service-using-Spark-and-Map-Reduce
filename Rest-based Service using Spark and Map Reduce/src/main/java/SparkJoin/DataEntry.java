package SparkJoin;

import java.io.Serializable;

public class DataEntry implements Serializable {
	private String[] attr = null;
	private int dimension;
	private double weight = 1;

	public DataEntry(int dimension) {
		this.attr = new String[dimension];
		this.dimension = dimension;
	}

	public int getDimension() {
		return dimension;
	}
	public String[] getAttr() {
		return attr;
	}
	public void setAttr(String[] ptrs) {
		this.attr = ptrs;
	}

	public double getWeight() {
		return this.weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	public String toString() {
		String result = "";

		for (int i = 0; i < attr.length; i++) {
			if (i != attr.length - 1)
				result += attr[i] + ",";
			else
				result += attr[i];
		}

		return result;
	}


 
}
