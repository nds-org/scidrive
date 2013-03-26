package edu.jhu.pha.vosync.meta;

public class Chunk {
	private String chunkId;
	private long chunkStart; // the position in full file = sum of all chunk sizes + 1
	private int chunkNum;
	private long size;
	public Chunk(String chunkId, long chunkStart, int chunkNum) {
		super();
		this.chunkId = chunkId;
		this.chunkStart = chunkStart;
		this.chunkNum = chunkNum;
	}
	public String getChunkId() {
		return chunkId;
	}
	public void setChunkId(String chunkId) {
		this.chunkId = chunkId;
	}
	public long getChunkStart() {
		return chunkStart;
	}
	public void setChunkStart(long chunkStart) {
		this.chunkStart = chunkStart;
	}
	public int getChunkNum() {
		return chunkNum;
	}
	public void setChunkNum(int chunkNum) {
		this.chunkNum = chunkNum;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	
}
