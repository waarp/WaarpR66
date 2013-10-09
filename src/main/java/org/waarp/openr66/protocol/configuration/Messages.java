package org.waarp.openr66.protocol.configuration;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.waarp.common.utility.SystemPropertyUtil;

public class Messages {
	private static final String BUNDLE_NAME = "messages"; //$NON-NLS-1$

	private static ResourceBundle RESOURCE_BUNDLE = null;

	static {
		String locale = SystemPropertyUtil.get(R66SystemProperties.OPENR66_LOCALE, "en");
		if (locale == null || locale.isEmpty()) {
			locale = "en";
		}
		init(new Locale(locale));
	}
	
	public static void init(Locale locale) {
		if (locale == null) {
			locale = new Locale("en");
		}
		RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME, locale);
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
	
	public static String getString(String key, Object ...args) {
		try {
			String source = RESOURCE_BUNDLE.getString(key);
			return MessageFormat.format(source, args);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
}
