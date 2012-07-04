/**
 * This file is part of Waarp Project.
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All Waarp Project is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with Waarp . If not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.protocol.utils;

import java.util.zip.Deflater;

/**
 * Utility that detects various properties specific to the current runtime environment, such as Java
 * version.
 * 
 * @author "Frederic Bregier"
 * 
 */
public class DetectionUtils {
	private static final int		JAVA_VERSION	= javaVersion0();
	private static final boolean	IS_WINDOWS;
	static {
		String os = System.getProperty("os.name").toLowerCase();
		// windows
		IS_WINDOWS = os.indexOf("win") >= 0;
	}

	/**
	 * Return <code>true</code> if the JVM is running on Windows
	 * 
	 */
	public static boolean isWindows() {
		return IS_WINDOWS;
	}

	public static int javaVersion() {
		return JAVA_VERSION;
	}

	private static int javaVersion0() {
		try {
			// Check if its android, if so handle it the same way as java6.
			//
			// See https://github.com/netty/netty/issues/282
			Class.forName("android.app.Application");
			return 6;
		} catch (ClassNotFoundException e) {
			// Ignore
		}

		try {
			Deflater.class.getDeclaredField("SYNC_FLUSH");
			return 7;
		} catch (Exception e) {
			// Ignore
		}

		try {
			Double.class.getDeclaredField("MIN_NORMAL");
			return 6;
		} catch (Exception e) {
			// Ignore
		}

		return 5;
	}
}
