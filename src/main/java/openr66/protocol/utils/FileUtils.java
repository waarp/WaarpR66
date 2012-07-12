/**
 * This file is part of GoldenGate Project (named also GoldenGate or GG).
 * 
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author tags. See the
 * COPYRIGHT.txt in the distribution for a full listing of individual contributors.
 * 
 * All GoldenGate Project is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 * 
 * GoldenGate is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with GoldenGate . If not,
 * see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.utils;

import goldengate.common.digest.FilesystemBasedDigest;
import goldengate.common.file.DirInterface;
import goldengate.common.file.filesystembased.FilesystemBasedFileParameterImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;

import openr66.protocol.configuration.Configuration;
import openr66.protocol.exception.OpenR66ProtocolSystemException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

/**
 * File Utils
 * 
 * @author Frederic Bregier
 * 
 */
public class FileUtils {
	/**
	 * 
	 * @param base
	 *            in absolute
	 * @param path
	 *            path in absolute or relative
	 * @return a new String for a file from the base and the path (according to relative or absolute
	 *         path)
	 * @throws OpenR66ProtocolSystemException
	 */
	public static String consolidatePath(String base, String path)
			throws OpenR66ProtocolSystemException {
		if (base == null || base.length() == 0 || path == null ||
				path.length() == 0) {
			throw new OpenR66ProtocolSystemException(
					"base and path must not be empty");
		}
		// First check if the path is relative or absolute
		String extDir = null;
		if (path.charAt(0) == DirInterface.SEPARATORCHAR) {
			extDir = Configuration.configuration.baseDirectory +
					DirInterface.SEPARATOR + path;
		} else {
			extDir = Configuration.configuration.baseDirectory +
					DirInterface.SEPARATOR + base + DirInterface.SEPARATOR +
					path;
		}
		return extDir;
	}

	/**
	 * Copy one file to another one
	 * 
	 * @param from
	 * @param to
	 * @param move
	 *            True if the copy is in fact a move operation
	 * @param append
	 *            True if the copy is in append
	 * @throws OpenR66ProtocolSystemException
	 */
	public static void copy(File from, File to, boolean move, boolean append)
			throws OpenR66ProtocolSystemException {
		if (from == null || to == null) {
			throw new OpenR66ProtocolSystemException(
					"Source or Destination is null");
		}
		File directoryTo = to.getParentFile();
		if (createDir(directoryTo)) {
			if (move && from.renameTo(to)) {
				return;
			}
			FileChannel fileChannelIn = getFileChannel(from, false, false);
			if (fileChannelIn == null) {
				throw new OpenR66ProtocolSystemException(
						"Cannot read source file");
				// return false;
			}
			FileChannel fileChannelOut = getFileChannel(to, true, append);
			if (fileChannelOut == null) {
				try {
					fileChannelIn.close();
				} catch (IOException e) {
				}
				throw new OpenR66ProtocolSystemException(
						"Cannot write destination file");
			}
			if (write(fileChannelIn, fileChannelOut) > 0) {
				if (move) {
					// do not test the delete
					from.delete();
				}
				return;
			}
			throw new OpenR66ProtocolSystemException("Cannot copy");
		}
		throw new OpenR66ProtocolSystemException(
				"Cannot access to parent dir of destination");
	}

	/**
	 * Copy a group of files to a directory
	 * 
	 * @param from
	 * @param directoryTo
	 * @param move
	 *            True if the copy is in fact a move operation
	 * @return the group of copy files or null (partially or totally) if an error occurs
	 * @throws OpenR66ProtocolSystemException
	 */
	public static File[] copy(File[] from, File directoryTo, boolean move)
			throws OpenR66ProtocolSystemException {
		if (from == null || directoryTo == null) {
			return null;
		}
		File[] to = null;
		if (createDir(directoryTo)) {
			to = new File[from.length];
			for (int i = 0; i < from.length; i++) {
				try {
					to[i] = copyToDir(from[i], directoryTo, move);
				} catch (OpenR66ProtocolSystemException e) {
					throw e;
				}
			}
		}
		return to;
	}

	/**
	 * Copy one file to a directory
	 * 
	 * @param from
	 * @param directoryTo
	 * @param move
	 *            True if the copy is in fact a move operation
	 * @return The copied file or null if an error occurs
	 * @throws OpenR66ProtocolSystemException
	 */
	public static File copyToDir(File from, File directoryTo, boolean move)
			throws OpenR66ProtocolSystemException {
		if (from == null || directoryTo == null) {
			throw new OpenR66ProtocolSystemException(
					"Source or Destination is null");
		}
		if (createDir(directoryTo)) {
			File to = new File(directoryTo, from.getName());
			if (move && from.renameTo(to)) {
				return to;
			}
			FileChannel fileChannelIn = getFileChannel(from, false, false);
			if (fileChannelIn == null) {
				throw new OpenR66ProtocolSystemException(
						"Cannot read source file");
			}
			FileChannel fileChannelOut = getFileChannel(to, true, false);
			if (fileChannelOut == null) {
				try {
					fileChannelIn.close();
				} catch (IOException e) {
				}
				throw new OpenR66ProtocolSystemException(
						"Cannot write destination file");
			}
			if (write(fileChannelIn, fileChannelOut) > 0) {
				if (move) {
					// do not test the delete
					from.delete();
				}
				return to;
			}
			throw new OpenR66ProtocolSystemException(
					"Cannot write destination file");
		}
		throw new OpenR66ProtocolSystemException(
				"Cannot access to parent dir of destination");
	}

	/**
	 * Create the directory associated with the File as path
	 * 
	 * @param directory
	 * @return True if created, False else.
	 */
	public static boolean createDir(File directory) {
		if (directory == null) {
			return false;
		}
		if (directory.isDirectory()) {
			return true;
		}
		return directory.mkdirs();
	}

	/**
	 * Delete physically the file
	 * 
	 * @param file
	 * @return True if OK, else if not (or if the file never exists).
	 */
	public static boolean delete(File file) {
		if (!file.exists()) {
			return true;
		}
		return file.delete();
	}

	/**
	 * Delete the directory associated with the File as path if empty
	 * 
	 * @param directory
	 * @return True if deleted, False else.
	 */
	public static boolean deleteDir(File directory) {
		if (directory == null) {
			return true;
		}
		if (!directory.exists()) {
			return true;
		}
		if (!directory.isDirectory()) {
			return false;
		}
		return directory.delete();
	}

	/**
	 * Delete physically the file but when the JVM exits (useful for temporary file)
	 * 
	 * @param file
	 */
	public static void deleteOnExit(File file) {
		if (!file.exists()) {
			return;
		}
		file.deleteOnExit();
	}

	/**
	 * Delete the directory and its subdirs associated with the File as path if empty
	 * 
	 * @param directory
	 * @return True if deleted, False else.
	 */
	public static boolean deleteRecursiveDir(File directory) {
		if (directory == null) {
			return true;
		}
		boolean retour = true;
		if (!directory.exists()) {
			return true;
		}
		if (!directory.isDirectory()) {
			return false;
		}
		File[] list = directory.listFiles();
		if (list == null || list.length == 0) {
			list = null;
			retour = directory.delete();
			return retour;
		}
		int len = list.length;
		for (int i = 0; i < len; i++) {
			if (list[i].isDirectory()) {
				if (!deleteRecursiveFileDir(list[i])) {
					retour = false;
				}
			} else {
				retour = false;
			}
		}
		list = null;
		if (retour) {
			retour = directory.delete();
		}
		return retour;
	}

	/**
	 * Delete the directory and its subdirs associated with the File dir if empty
	 * 
	 * @param dir
	 * @return True if deleted, False else.
	 */
	private static boolean deleteRecursiveFileDir(File dir) {
		if (dir == null) {
			return true;
		}
		boolean retour = true;
		if (!dir.exists()) {
			return true;
		}
		File[] list = dir.listFiles();
		if (list == null || list.length == 0) {
			list = null;
			return dir.delete();
		}
		int len = list.length;
		for (int i = 0; i < len; i++) {
			if (list[i].isDirectory()) {
				if (!deleteRecursiveFileDir(list[i])) {
					retour = false;
				}
			} else {
				retour = false;
				list = null;
				return retour;
			}
		}
		list = null;
		if (retour) {
			retour = dir.delete();
		}
		return retour;
	}

	/**
	 * @param _FileName
	 * @param _Path
	 * @return true if the file exist in the specified path
	 */
	public static boolean FileExist(String _FileName, String _Path) {
		boolean exist = false;
		String fileString = _Path + File.separator + _FileName;
		File file = new File(fileString);
		if (file.exists()) {
			exist = true;
		}
		return exist;
	}

	/**
	 * Returns the FileChannel in Out MODE (if isOut is True) or in In MODE (if isOut is False)
	 * associated with the file. In out MODE, it can be in append MODE.
	 * 
	 * @param isOut
	 * @param append
	 * @return the FileChannel (OUT or IN)
	 * @throws OpenR66ProtocolSystemException
	 */
	private static FileChannel getFileChannel(File file, boolean isOut,
			boolean append) throws OpenR66ProtocolSystemException {
		FileChannel fileChannel = null;
		try {
			if (isOut) {
				FileOutputStream fileOutputStream = new FileOutputStream(file
						.getPath(), append);
				fileChannel = fileOutputStream.getChannel();
				if (append) {
					// Bug in JVM since it does not respect the API (position
					// should be set as length)
					try {
						fileChannel.position(file.length());
					} catch (IOException e) {
					}
				}
			} else {
				if (!file.exists()) {
					throw new OpenR66ProtocolSystemException(
							"File does not exist");
				}
				FileInputStream fileInputStream = new FileInputStream(file
						.getPath());
				fileChannel = fileInputStream.getChannel();
			}
		} catch (FileNotFoundException e) {
			throw new OpenR66ProtocolSystemException("File not found", e);
		}
		return fileChannel;
	}

	/**
	 * Get the list of files from a given directory
	 * 
	 * @param directory
	 * @return the list of files (as an array)
	 */
	public static File[] getFiles(File directory) {
		if (directory == null || !directory.isDirectory()) {
			return null;
		}
		return directory.listFiles();
	}

	/**
	 * Get the list of files from a given directory and a filter
	 * 
	 * @param directory
	 * @param filter
	 * @return the list of files (as an array)
	 */
	public static File[] getFiles(File directory, FilenameFilter filter) {
		if (directory == null || !directory.isDirectory()) {
			return null;
		}
		return directory.listFiles(filter);
	}

	/**
	 * Calculates and returns the hash of the contents of the given file.
	 * 
	 * @param f
	 *            FileInterface to hash
	 * @return the hash from the given file
	 * @throws OpenR66ProtocolSystemException
	 **/
	public static String getHash(File f) throws OpenR66ProtocolSystemException {
		try {
			return FilesystemBasedDigest.getHex(FilesystemBasedDigest.getHash(f,
					FilesystemBasedFileParameterImpl.useNio, Configuration.configuration.digest));
		} catch (IOException e) {
			throw new OpenR66ProtocolSystemException(e);
		}
	}

	/**
	 * 
	 * @param buffer
	 * @return the hash from the given Buffer
	 */
	public static ChannelBuffer getHash(ChannelBuffer buffer) {
		byte[] newkey;
		try {
			newkey = FilesystemBasedDigest.getHash(buffer, Configuration.configuration.digest);
		} catch (IOException e) {
			return ChannelBuffers.EMPTY_BUFFER;
		}
		return ChannelBuffers.wrappedBuffer(newkey);
	}

	/**
	 * Write one fileChannel to another one. Close the fileChannels
	 * 
	 * @param fileChannelIn
	 *            source of file
	 * @param fileChannelOut
	 *            destination of file
	 * @return The size of copy if is OK
	 * @throws AtlasIoException
	 */
	private static long write(FileChannel fileChannelIn,
			FileChannel fileChannelOut) throws OpenR66ProtocolSystemException {
		if (fileChannelIn == null) {
			if (fileChannelOut != null) {
				try {
					fileChannelOut.close();
				} catch (IOException e) {
				}
			}
			throw new OpenR66ProtocolSystemException("FileChannelIn is null");
		}
		if (fileChannelOut == null) {
			try {
				fileChannelIn.close();
			} catch (IOException e) {
			}
			throw new OpenR66ProtocolSystemException("FileChannelOut is null");
		}
		long size = 0;
		long transfert = 0;
		try {
			long position = fileChannelOut.position();
			size = fileChannelIn.size();
			transfert = fileChannelOut.transferFrom(fileChannelIn, position,
					size);
		} catch (IOException e) {
			try {
				fileChannelOut.close();
				fileChannelIn.close();
			} catch (IOException e1) {
			}
			throw new OpenR66ProtocolSystemException(
					"An error during copy occurs", e);
		}
		try {
			fileChannelOut.close();
			fileChannelIn.close();
		} catch (IOException e) {// Close error can be ignored
		}
		boolean retour = size == transfert;
		if (!retour) {
			throw new OpenR66ProtocolSystemException("Copy is not complete: " +
					transfert + " bytes instead of " + size + " original bytes");
		}
		return size;
	}
}
