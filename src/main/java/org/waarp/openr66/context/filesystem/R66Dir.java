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
package org.waarp.openr66.context.filesystem;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.waarp.common.command.exception.CommandAbstractException;
import org.waarp.common.command.exception.Reply550Exception;
import org.waarp.common.command.exception.Reply553Exception;
import org.waarp.common.file.filesystembased.FilesystemBasedDirImpl;
import org.waarp.common.file.filesystembased.FilesystemBasedOptsMLSxImpl;
import org.waarp.common.file.filesystembased.specific.FilesystemBasedCommonsIo;
import org.waarp.common.file.filesystembased.specific.FilesystemBasedDirJdkAbstract;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.authentication.R66Auth;
import org.waarp.openr66.protocol.configuration.Configuration;

/**
 * Directory representation
 * 
 * @author frederic bregier
 * 
 */
public class R66Dir extends FilesystemBasedDirImpl {

	/**
	 * @param session
	 */
	public R66Dir(R66Session session) {
		super(session, new FilesystemBasedOptsMLSxImpl());
	}

	public R66File newFile(String path, boolean append)
			throws CommandAbstractException {
		return new R66File((R66Session) getSession(), this, path, append);
	}

	/**
	 * Same as setUnique() except that File will be prefixed by id and postfixed by filename
	 * 
	 * @param prefix
	 * @param filename
	 * @return the R66File with a unique filename and a temporary extension
	 * @throws CommandAbstractException
	 */
	public synchronized R66File setUniqueFile(long prefix, String filename)
			throws CommandAbstractException {
		checkIdentify();
		File file = null;
		String prename = prefix + "_";
		if (prename.length() < 3) {
			prename = "xx_" + prename;
		}
		String basename = R66File.getBasename(filename);
		try {
			file = File.createTempFile(prename, "_" + basename +
					Configuration.EXT_R66, getFileFromPath(currentDir));
		} catch (IOException e) {
			throw new Reply550Exception("Cannot create unique file from " +
					basename);
		}
		String currentFile = getRelativePath(file);
		return newFile(normalizePath(currentFile), false);
	}

	/**
	 * 
	 * @param file
	 * @return the final unique basename without the temporary extension
	 */
	public static String getFinalUniqueFilename(R66File file) {
		String finalpath = file.getBasename();
		int pos = finalpath.lastIndexOf(Configuration.EXT_R66);
		if (pos > 0) {
			finalpath = finalpath.substring(0, pos);
		}
		return finalpath;
	}

	/**
	 * Finds all files matching a wildcard expression (based on '?', '~' or '*') but without
	 * checking BusinessPath, thus returning absolute path.
	 * 
	 * @param pathWithWildcard
	 *            The wildcard expression with a business path.
	 * @return List of String as relative paths matching the wildcard expression. Those files are
	 *         tested as valid from business point of view. If Wildcard support is not active, if
	 *         the path contains any wildcards, it will throw an error.
	 * @throws CommandAbstractException
	 */
	protected List<String> wildcardFilesNoCheck(String pathWithWildcard)
			throws CommandAbstractException {
		List<String> resultPaths = new ArrayList<String>();
		// First check if pathWithWildcard contains wildcards
		if (!(pathWithWildcard.contains("*") || pathWithWildcard.contains("?") || pathWithWildcard
				.contains("~"))) {
			// No so simply return the list containing this path
			resultPaths.add(pathWithWildcard);
			return resultPaths;
		}
		// Do we support Wildcard path
		if (!FilesystemBasedDirJdkAbstract.ueApacheCommonsIo) {
			throw new Reply553Exception("Wildcards in pathname is not allowed");
		}
		File wildcardFile = new File(pathWithWildcard);
		File rootFile;
		initWindowsSupport();
		if (ISUNIX) {
			rootFile = new File("/");
		} else {
			rootFile = getCorrespondingRoot(wildcardFile);
		}
		// Split wildcard path into subdirectories.
		List<String> subdirs = new ArrayList<String>();
		while (wildcardFile != null) {
			File parent = wildcardFile.getParentFile();
			if (parent == null) {
				subdirs.add(0, wildcardFile.getPath());
				break;
			}
			subdirs.add(0, wildcardFile.getName());
			if (parent.equals(rootFile)) {
				// End of wildcard path
				subdirs.add(0, parent.getPath());
				break;
			}
			wildcardFile = parent;
		}
		List<File> basedPaths = new ArrayList<File>();
		// First set root
		basedPaths.add(new File(subdirs.get(0)));
		int i = 1;
		// For each wilcard subdirectory
		while (i < subdirs.size()) {
			// Set current filter
			FileFilter fileFilter = FilesystemBasedCommonsIo
					.getWildcardFileFilter(subdirs.get(i));
			List<File> newBasedPaths = new ArrayList<File>();
			// Look for matches in all the current search paths
			for (File dir : basedPaths) {
				if (dir.isDirectory()) {
					for (File match : dir.listFiles(fileFilter)) {
						newBasedPaths.add(match);
					}
				}
			}
			// base Search Path changes now
			basedPaths = newBasedPaths;
			i++;
		}
		// Valid each file first
		for (File file : basedPaths) {
			resultPaths.add(file.getAbsolutePath());
		}
		return resultPaths;
	}

	/**
	 * Create a new file according to the path without checking BusinessPath, so as external File.
	 * 
	 * @param path
	 * @return the File created
	 * @throws CommandAbstractException
	 */
	public R66File setFileNoCheck(String path)
			throws CommandAbstractException {
		checkIdentify();
		String newpath = consolidatePath(path);
		List<String> paths = wildcardFilesNoCheck(newpath);
		if (paths.size() != 1) {
			throw new Reply550Exception("File not found from: " + newpath +" and " +
					paths.size() + " founds");
		}
		String extDir = paths.get(0);
		return new R66File((R66Session) getSession(), this, extDir);
	}

	/**
	 * This method returns the Full path for the current directory
	 * 
	 * @return the full path associated with the current Dir
	 */
	public String getFullPath() {
		if (session.getAuth() == null) {
			return currentDir;
		}
		return ((R66Auth) session.getAuth()).getAbsolutePath(currentDir);
	}

	@Override
	public String toString() {
		return "Dir: " + currentDir;
	}
}
