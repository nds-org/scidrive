package edu.jhu.pha.vospace.process.tika;

public class ATpyType {
	public static final int UNKNOWN = 0;
	public static final int INT = 1;
	public static final int FLOAT = 2;
	public static final int STRING = 3;
	
	int kind;
	int itemSize;
	
	public ATpyType(int kind, int itemSize) {
		this.kind = kind;
		this.itemSize = itemSize;
	}
	
	public int getKind() {
		return kind;
	}
	
	public int getItemSize() {
		return itemSize;
	}
}
