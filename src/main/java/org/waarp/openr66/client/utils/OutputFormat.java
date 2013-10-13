/**
   This file is part of Waarp Project.

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All Waarp Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   Waarp is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Waarp .  If not, see <http://www.gnu.org/licenses/>.
 */
package org.waarp.openr66.client.utils;

import java.io.IOException;
import java.util.Map;

import org.waarp.common.json.JsonHandler;
import org.waarp.openr66.protocol.configuration.Messages;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This method allows to format output for Waarp R66 clients
 * 
 * @author "Frederic Bregier"
 *
 */
public class OutputFormat extends JsonHandler {
	public static enum OUTPUTFORMAT {
		QUIET,
		JSON,
		XML,
		PROPERTY,
		CSV
	}
	
	public static OUTPUTFORMAT defaultOutput = OUTPUTFORMAT.JSON;

	public static enum FIELDS {
		command, args, status, statusTxt, transfer, error, remote
	}
	
	/**
	 * Helper to set the output format desired for the command
	 * @param args
	 */
	public static void getParams(String []args) {
		for (int i = 1; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-quiet")) {
				defaultOutput = OUTPUTFORMAT.QUIET;
			} else if (args[i].equalsIgnoreCase("-xml")) {
				defaultOutput = OUTPUTFORMAT.XML;
			} else if (args[i].equalsIgnoreCase("-csv")) {
				defaultOutput = OUTPUTFORMAT.CSV;
			} else if (args[i].equalsIgnoreCase("-json")) {
				defaultOutput = OUTPUTFORMAT.JSON;
			} else if (args[i].equalsIgnoreCase("-property")) {
				defaultOutput = OUTPUTFORMAT.PROPERTY;
			}
		}
	}
	
	private OUTPUTFORMAT format = defaultOutput;
	private ObjectNode node = createObjectNode();
	
	public OutputFormat(String command, String []args) {
		setValue(FIELDS.command.name(), command);
		if (args != null) {
			StringBuilder builder = new StringBuilder();
			for (String string : args) {
				builder.append(string);
				builder.append(' ');
			}
			setValue(FIELDS.args.name(), builder.toString());
		}
	}
	
	public void setFormat(OUTPUTFORMAT format) {
		this.format = format;
	}

	/**
	 * 
	 * @param field
	 * @param value
	 */
	public void setValue(Map<String, Object> values) {
		String json = writeAsString(values);
		ObjectNode temp = getFromString(json);
		node.putAll(temp);
	}

	public void setValueString(Map<String, String> values) {
		String json = writeAsString(values);
		ObjectNode temp = getFromString(json);
		node.putAll(temp);
	}

	/**
	 * 
	 * @param field
	 * @param value
	 */
	public void setValue(String field, boolean value) {
		setValue(node, field, value);
	}

	/**
	 * 
	 * @param field
	 * @param value
	 */
	public final void setValue(String field, double value) {
		setValue(node, field, value);
	}

	/**
	 * 
	 * @param field
	 * @param value
	 */
	public final void setValue(String field, int value) {
		setValue(node, field, value);
	}

	/**
	 * 
	 * @param field
	 * @param value
	 */
	public final void setValue(String field, long value) {
		setValue(node, field, value);
	}

	/**
	 * 
	 * @param field
	 * @param value
	 */
	public final void setValue(String field, String value) {
		setValue(node, field, value);
	}

	/**
	 * 
	 * @param field
	 * @param value
	 */
	public final void setValue(String field, byte []value) {
		setValue(node, field, value);
	}
	
	/**
	 * 
	 * @param field
	 * @return True if all fields exist
	 */
	public final boolean exist(String ...field) {
		return exist(node, field);
	}

	/**
	 * 
	 * @return True if the current default output format is on QUIET
	 */
	public static boolean isQuiet() {
		return defaultOutput == OUTPUTFORMAT.QUIET;
	}
	/**
	 * Helper for sysOut
	 */
	public void sysout() {
		if (format != OUTPUTFORMAT.QUIET) {
			System.out.println(getContext());
			System.out.println(this.toString(format));
		}
	}
	
	private String getContext() {
		return "[" + getValue(node, FIELDS.command, "") + "] " +getValue(node, FIELDS.statusTxt, "");
	}
	/**
	 * Helper for Logger
	 * @return the String to print in logger
	 */
	public String loggerOut() {
		return getContext() + " => " + toString(OUTPUTFORMAT.JSON);
	}
	
	@Override
	public String toString() {
		return toString(format);
	}
	
	/**
	 * Helper to get string representation of the current object
	 * @param format
	 * @return the String representation
	 */
	public String toString(OUTPUTFORMAT format) {
		String inString = writeAsString(node);
		switch (format) {
			case QUIET:
			case JSON:
				return inString;
			case CSV:
				try {
					Map<String, Object> map =  mapper.readValue(inString, new TypeReference<Map<String, Object>>() {});
					StringBuilder builderKeys = new StringBuilder();
					StringBuilder builderValues = new StringBuilder();
					boolean next = false;
					for (String key : map.keySet()) {
						if (next) {
							builderKeys.append(';');
							builderValues.append(';');
						} else {
							next = true;
						}
						builderKeys.append(key);
						builderValues.append(map.get(key));
					}
					return builderKeys.toString()+"\n"+builderValues.toString();
				} catch (JsonParseException e) {
					return Messages.getString("Message.CantConvert", "CSV") + inString;
				} catch (JsonMappingException e) {
					return Messages.getString("Message.CantConvert", "CSV") +inString;
				} catch (IOException e) {
					return Messages.getString("Message.CantConvert", "CSV") +inString;
				}
			case PROPERTY:
				try {
					Map<String, Object> map =  mapper.readValue(inString, new TypeReference<Map<String, Object>>() {});
					StringBuilder builder = new StringBuilder();
					boolean next = false;
					for (String key : map.keySet()) {
						if (next) {
							builder.append('\n');
						} else {
							next = true;
						}
						builder.append(key);
						builder.append('=');
						builder.append(map.get(key));
					}
					return builder.toString();
				} catch (JsonParseException e) {
					return Messages.getString("Message.CantConvert", "PROPERTY") +inString;
				} catch (JsonMappingException e) {
					return Messages.getString("Message.CantConvert", "PROPERTY") +inString;
				} catch (IOException e) {
					return Messages.getString("Message.CantConvert", "PROPERTY") +inString;
				}
			case XML:
				try {
					Map<String, Object> map =  mapper.readValue(inString, new TypeReference<Map<String, Object>>() {});
					StringBuilder builder = new StringBuilder("<xml>");
					for (String key : map.keySet()) {
						builder.append('<');
						builder.append(key);
						builder.append('>');
						builder.append(map.get(key));
						builder.append("</");
						builder.append(key);
						builder.append('>');
					}
					builder.append("</xml>");
					return builder.toString();
				} catch (JsonParseException e) {
					return Messages.getString("Message.CantConvert", "XML") +inString;
				} catch (JsonMappingException e) {
					return Messages.getString("Message.CantConvert", "XML") +inString;
				} catch (IOException e) {
					return Messages.getString("Message.CantConvert", "XML") +inString;
				}
			default:
				return inString;
		}
	}
}
