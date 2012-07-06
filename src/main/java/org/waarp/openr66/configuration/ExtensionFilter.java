package org.waarp.openr66.configuration;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Extension Filter based on extension
 * 
 * @author Frederic Bregier
 * 
 */
public class ExtensionFilter implements FilenameFilter {
	String filter = RuleFileBasedConfiguration.EXT_RULE;

	public ExtensionFilter(String filter) {
		this.filter = filter;
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
	 */
	@Override
	public boolean accept(File arg0, String arg1) {
		return arg1.endsWith(filter);
	}

}
