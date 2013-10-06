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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.channels.FileChannel;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.waarp.common.command.exception.CommandAbstractException;
import org.waarp.common.digest.FilesystemBasedDigest;
import org.waarp.common.digest.FilesystemBasedDigest.DigestAlgo;
import org.waarp.common.file.filesystembased.FilesystemBasedFileParameterImpl;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.utility.DetectionUtils;
import org.waarp.common.utility.WaarpStringUtils;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.filesystem.R66Dir;
import org.waarp.openr66.context.filesystem.R66File;
import org.waarp.openr66.context.task.exception.OpenR66RunnerErrorException;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.exception.OpenR66ProtocolSystemException;

/**
 * File Utils
 * 
 * @author Frederic Bregier
 * 
 */
public class FileUtils {

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
	public final static boolean createDir(File directory) {
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
	public final static boolean delete(File file) {
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
	public final static boolean deleteDir(File directory) {
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
	public final static void deleteOnExit(File file) {
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
	 * Change or create the R66File associated with the context
	 * @param logger
	 * @param session
	 * @param filenameSrc new filename
	 * @param isPreStart
	 * @param isSender
	 * @param isThrough
	 * @param file old R66File if any (might be null)
	 * @return the R66File
	 * @throws OpenR66RunnerErrorException
	 */
	public final static R66File getFile(WaarpInternalLogger logger, R66Session session, String filenameSrc, 
			boolean isPreStart, boolean isSender, boolean isThrough, R66File file) throws OpenR66RunnerErrorException {
		String filename;
		logger.debug("PreStart: "+isPreStart);
		logger.debug("Dir is: "+session.getDir().getFullPath());
		logger.debug("File is: "+filenameSrc);
		if (isPreStart) {
			filename = R66Dir.normalizePath(filenameSrc);
			if (filename.startsWith("file:/")) {
				if (DetectionUtils.isWindows()) {
					filename = filename.substring("file:/".length());
				} else {
					filename = filename.substring("file:".length());
				}
				try {
					filename = URLDecoder.decode(filename, WaarpStringUtils.UTF8.name());
				} catch (UnsupportedEncodingException e) {
					throw new OpenR66RunnerErrorException("File cannot be read: " +
							filename);
				}
			}
			logger.debug("File becomes: "+filename);
		} else {
			filename = filenameSrc;
		}
		if (isSender) {
			try {
				if (file == null) {
					try {
						file = (R66File) session.getDir().setFile(filename, false);
					} catch (CommandAbstractException e) {
						logger.warn("File not placed in normal directory", e);
						// file is not under normal base directory, so is external
						// File should already exist but can be using special code ('*?')
						file = session.getDir().setFileNoCheck(filename);
					}
				}
				if (isThrough) {
					// no test on file since it does not really exist
					logger.debug("File is in through mode: {}", file);
				} else if (!file.canRead()) {
					// file is not under normal base directory, so is external
					// File should already exist but cannot use special code ('*?')
					file = new R66File(session, session.getDir(), filename);
					if (!file.canRead()) {
						throw new OpenR66RunnerErrorException("File cannot be read: " +
								file.getTrueFile().getAbsolutePath());
					}
				}
			} catch (CommandAbstractException e) {
				throw new OpenR66RunnerErrorException(e);
			}
		} else {
			// not sender so file is just registered as is but no test of existence
			file = new R66File(session, session.getDir(), filename);
		}
		return file;
	}

	/**
	 * @param _FileName
	 * @param _Path
	 * @return true if the file exist in the specified path
	 */
	public final static boolean FileExist(String _FileName, String _Path) {
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
				@SuppressWarnings("resource")
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
				@SuppressWarnings("resource")
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
	public final static File[] getFiles(File directory) {
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
	public final static File[] getFiles(File directory, FilenameFilter filter) {
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
	public final static String getHash(File f) throws OpenR66ProtocolSystemException {
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
	public final static ChannelBuffer getHash(ChannelBuffer buffer, DigestAlgo algo) {
		byte[] newkey;
		try {
			newkey = FilesystemBasedDigest.getHash(buffer, algo);
		} catch (IOException e) {
			return ChannelBuffers.EMPTY_BUFFER;
		}
		return ChannelBuffers.wrappedBuffer(newkey);
	}
	
	/**
	 * Compute global hash (if possible)
	 * @param digest
	 * @param buffer
	 */
	public static void computeGlobalHash(FilesystemBasedDigest digest, ChannelBuffer buffer) {
		if (digest == null) {
			return;
		}
		byte[] bytes = null;
		int start = 0;
		int length = buffer.readableBytes();
		if (buffer.hasArray()) {
			start = buffer.arrayOffset();
			bytes = buffer.array();
			if (bytes.length > start + length) {
				byte[] temp = new byte[length];
				System.arraycopy(bytes, start, temp, 0, length);
				start = 0;
				bytes = temp;
			}
		} else {
			bytes = new byte[length];
			buffer.getBytes(buffer.readerIndex(), bytes);
		}
		digest.Update(bytes, start, length);
	}

	/**
	 * Compute global hash (if possible) from a file but up to length
	 * @param digest
	 * @param file
	 * @param length
	 */
	public static void computeGlobalHash(FilesystemBasedDigest digest, File file, int length) {
		if (digest == null) {
			return;
		}
		byte[] bytes = new byte[65536];
		int still = length;
		int len = still > 65536 ? 65536 : still;
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(file);
			while (inputStream.read(bytes, 0, len) > 0) {
				digest.Update(bytes, 0, len);
				still -= length;
				if (still <= 0) {
					break;
				}
				len = still > 65536 ? 65536 : still;
			}
		} catch (FileNotFoundException e) {
			// error
		} catch (IOException e) {
			// error
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
				}
			}
		}
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
			transfert = fileChannelOut.position();
			size = fileChannelIn.size();
			int chunkSize = 8192;
			while (transfert < size) {
				if (chunkSize < size - transfert) {
					chunkSize = (int) (size - transfert);
				}
				transfert += fileChannelOut.transferFrom(fileChannelIn, transfert, chunkSize);
			}
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
