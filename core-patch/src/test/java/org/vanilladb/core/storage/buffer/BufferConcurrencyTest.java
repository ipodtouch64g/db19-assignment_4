/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.buffer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.util.BarrierStartRunner;

import junit.framework.Assert;

public class BufferConcurrencyTest {
	private static Logger logger = Logger.getLogger(BufferConcurrencyTest.class.getName());

	private static final int PINNING_CLIENT_COUNT = 100;
	private static final int SETVAL_CLIENT_COUNT = 10;

	@BeforeClass
	public static void init() {
		ServerInit.init(BufferConcurrencyTest.class);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN BUFFER CONCURRENCY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH BUFFER CONCURRENCY TEST");
	}

	@Test
	public void testConcourrentPinning() {
		Buffer buffer = new Buffer();
		CyclicBarrier startBarrier = new CyclicBarrier(PINNING_CLIENT_COUNT);
		CyclicBarrier endBarrier = new CyclicBarrier(PINNING_CLIENT_COUNT + 1);

		// Create multiple threads
		for (int i = 0; i < PINNING_CLIENT_COUNT; i++)
			new Pinner(startBarrier, endBarrier, buffer).start();

		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

		// Check the results
		Assert.assertEquals("testBufferPinCount failed", false, buffer.isPinned());
	}
	
	/**
	 * Idea: Concurrent set strings on multiple offsets.
	 */
	@Test
	public void testConcourrentGetAndSetVals() {
		Buffer buffer = new Buffer();

		// Create multiple threads for setting values
		CyclicBarrier startBarrier = new CyclicBarrier(SETVAL_CLIENT_COUNT);
		CyclicBarrier endBarrier = new CyclicBarrier(SETVAL_CLIENT_COUNT + 1);
		for (int i = 0; i < SETVAL_CLIENT_COUNT; i++)
			new Setter(startBarrier, endBarrier, buffer, i).start();

		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
		
		// Create multiple threads for getting values
		startBarrier = new CyclicBarrier(SETVAL_CLIENT_COUNT);
		endBarrier = new CyclicBarrier(SETVAL_CLIENT_COUNT + 1);
		List<Getter> getters = new ArrayList<Getter>();
		for (int i = 0; i < SETVAL_CLIENT_COUNT; i++) {
			Getter getter = new Getter(startBarrier, endBarrier, buffer, i);
			getters.add(getter);
			getter.start();
		}
		
		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
				
		// Check the results
		for (Getter getter : getters) {
			if (getter.foundError) {
				Assert.assertEquals("test concurrent setVal failed", getter.expStrCon,
						getter.foundStrCon);
			}
			if (getter.getException() != null) {
				Assert.fail("Exception happens when getting value for '" + getter.expStrCon
						+ "'\n" + getter.getException());
			}
			
		}
		LogSeqNum lsn = new LogSeqNum(1, (SETVAL_CLIENT_COUNT - 1) * Type.VARCHAR(10).maxSize());
		Assert.assertEquals("test concurrent setVal failed", lsn, buffer.lastLsn());
	}

	class Pinner extends BarrierStartRunner {

		Buffer buf;

		public Pinner(CyclicBarrier startBarrier, CyclicBarrier endBarrier, Buffer buf) {
			super(startBarrier, endBarrier);

			this.buf = buf;
		}

		@Override
		public void runTask() {
			for (int i = 0; i < 10000; i++) {
				buf.pin();
				buf.unpin();
			}
		}

	}
	
	class Setter extends BarrierStartRunner {

		Buffer buf;
		int offset;
		Constant strCon;
		LogSeqNum lsn;
		long txNum;

		public Setter(CyclicBarrier startBarrier, CyclicBarrier endBarrier, Buffer buf,
				int id) {
			super(startBarrier, endBarrier);

			this.buf = buf;
			Type type = Type.VARCHAR(10);
			this.offset = id * type.maxSize();
			this.strCon = new VarcharConstant("Val " + id, type);
			this.lsn = new LogSeqNum(1, offset);
			this.txNum = id;
		}

		@Override
		public void runTask() {
			for (int i = 0; i < 10000; i++) {
				buf.setVal(offset, strCon, txNum, lsn);
			}
		}

	}
	
	class Getter extends BarrierStartRunner {

		Buffer buf;
		int offset;
		Type type;
		Constant expStrCon;
		boolean foundError;
		Constant foundStrCon;

		public Getter(CyclicBarrier startBarrier, CyclicBarrier endBarrier, Buffer buf,
				int id) {
			super(startBarrier, endBarrier);

			this.buf = buf;
			this.type = Type.VARCHAR(10);
			this.offset = id * type.maxSize();
			this.expStrCon = new VarcharConstant("Val " + id, type);
		}

		@Override
		public void runTask() {
			for (int i = 0; i < 10000; i++) {
				Constant val = buf.getVal(offset, type);
				if (!val.equals(expStrCon)) {
					foundError = true;
					foundStrCon = val;
					break;
				}
			}
		}
	}
}
