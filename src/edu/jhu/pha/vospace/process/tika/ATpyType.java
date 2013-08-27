package edu.jhu.pha.vospace.process.tika;

public class ATpyType {
	public enum Type {UNKNOWN, INT, FLOAT, STRING}
	
	Type kind;
	int itemSize;
	
	public ATpyType(Type kind, int itemSize) {
		this.kind = kind;
		this.itemSize = itemSize;
	}
	
	public Type getKind() {
		return kind;
	}
	
	public int getItemSize() {
		return itemSize;
	}
}
