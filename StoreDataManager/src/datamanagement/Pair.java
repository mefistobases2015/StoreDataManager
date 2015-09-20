package datamanagement;

public class Pair <T1, T2>{

	private T1 first;
	private T2 second;
	
	public Pair(T1 first, T2 second){
		this.first = first;
		this.second = second;
	}

	/**
	 * @return the first
	 */
	public T1 getFirst() {
		return first;
	}

	/**
	 * @param first the first to set
	 */
	public void setFirst(T1 first) {
		this.first = first;
	}

	/**
	 * @return the second
	 */
	public T2 getSecond() {
		return second;
	}

	/**
	 * @param second the second to set
	 */
	public void setSecond(T2 second) {
		this.second = second;
	}
}
