/*******************************************************************************
 * Copyright 2016 vanilladb.org
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
package org.vanilladb.core.storage.file;

/**
 * A reference to a disk block. A BlockId object consists of a fileName and a
 * block number. It does not hold the contents of the block; instead, that is
 * the job of a {@link Page} object.
 */
public class BlockId {
	private String fileName;
	private long blkNum;
	private int hashcode;
	private String str;
	/**
	 * Constructs a block ID for the specified fileName and block number.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param blkNum
	 *            the block number
	 */
	public BlockId(String fileName, long blkNum) {
		this.fileName = fileName;
		this.blkNum = blkNum;
		// calculate hashCode here to save time~~~
		str = "[file " + fileName + ", block " + blkNum + "]";
		hashcode = str.hashCode();
	}

	/**
	 * Returns the name of the file where the block lives.
	 * 
	 * @return the fileName
	 */
	public String fileName() {
		return fileName;
	}

	/**
	 * Returns the location of the block within the file.
	 * 
	 * @return the block number
	 */
	public long number() {
		return blkNum;
	}

	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(BlockId.class)))
			return false;
		BlockId blk = (BlockId) obj;
		return fileName.equals(blk.fileName) && blkNum == blk.blkNum;
	}

	public String toString() {
		return str;
	}

	public int hashCode() {
		return hashcode;
	}
}
