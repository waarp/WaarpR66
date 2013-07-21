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
package org.waarp.openr66.protocol.configuration;

import java.io.IOException;

import org.waarp.common.digest.FilesystemBasedDigest.DigestAlgo;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.protocol.utils.Version;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Partner Configuration
 * @author "Frederic Bregier"
 *
 */
public class PartnerConfiguration {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(PartnerConfiguration.class);
	/**
	 * Here is the list of specific identified version
	 */
	public static enum R66VERSION {
		R66VERSION_NOUSABLE("2.4.12"), R66VERSION_SEPARATOR("2.4.13"), R66VERSION_2_4_17("2.4.17");
		
		public String version;
		private R66VERSION(String name) {
			this.version = name;
		}
	}
	/**
	 * Uses as separator in field
	 */
	public static final String BAR_SEPARATOR_FIELD = ";";
	/**
	 * Uses as separator in field
	 */
	public static final String BLANK_SEPARATOR_FIELD = " ";
	/**
	 * Uses as separator in field
	 */
	public static String SEPARATOR_FIELD = BAR_SEPARATOR_FIELD;

	/**
	 * JSON Fields
	 *
	 */
	public static enum FIELDS {
		HOSTID("nohostid"), VERSION(R66VERSION.R66VERSION_NOUSABLE.version), 
		DIGESTALGO(DigestAlgo.MD5.name), FILESIZE(false), FINALHASH(false), 
		PROXIFIED(false), SEPARATOR(BLANK_SEPARATOR_FIELD);
		
		String name;
		Object defaultValue;
		private FIELDS(Object def) {
			this.name = name();
			this.defaultValue = def;
		}
	}
	/**
	 * JSON parser
	 */
	public static ObjectMapper mapper = new ObjectMapper();
	
	private String id;
	private JsonNode root = mapper.createObjectNode();
	
	/**
	 * Constructor for an external HostId
	 * @param id
	 * @param version
	 */
	public PartnerConfiguration(String id, String json) {
		this.id = id;
		((ObjectNode) root).put(FIELDS.HOSTID.name, id);
		int pos = json.lastIndexOf('{');
		String version = null;
		if (pos > 1) {
			version = json.substring(0, pos-1);
		} else {
			version = json;
		}
		((ObjectNode) root).put(FIELDS.VERSION.name, version);
		if (isVersion2GEQVersion1("2.4.12", version)) {
			((ObjectNode) root).put(FIELDS.FILESIZE.name, true);
			((ObjectNode) root).put(FIELDS.FINALHASH.name, true);
		} else {
			((ObjectNode) root).put(FIELDS.FILESIZE.name, (Boolean) FIELDS.FILESIZE.defaultValue);
			((ObjectNode) root).put(FIELDS.FINALHASH.name, (Boolean) FIELDS.FINALHASH.defaultValue);
		}
		((ObjectNode) root).put(FIELDS.DIGESTALGO.name, (String) FIELDS.DIGESTALGO.defaultValue);
		((ObjectNode) root).put(FIELDS.PROXIFIED.name, (Boolean) FIELDS.PROXIFIED.defaultValue);
		String sep = SEPARATOR_FIELD;
		if (! isVersion2GEQVersion1(R66VERSION.R66VERSION_SEPARATOR.version, version)) {
			sep = BLANK_SEPARATOR_FIELD;
		}
		((ObjectNode) root).put(FIELDS.SEPARATOR.name, sep);
		
		if (json != null && pos > 1) {
			String realjson = json.substring(pos);
			try {
				JsonNode info = mapper.readTree(realjson);
				if (info != null) {
					((ObjectNode) root).putAll((ObjectNode) info);
				}
			} catch (IOException e1) {
			}
		}
		logger.debug("Info HostId: "+root.toString());
	}
	
	/**
	 * Self constructor
	 * @param id 
	 */
	public PartnerConfiguration(String id) {
		this.id = id;
		((ObjectNode) root).put(FIELDS.HOSTID.name, id);
		((ObjectNode) root).put(FIELDS.VERSION.name, Version.ID);
		((ObjectNode) root).put(FIELDS.FILESIZE.name, true);
		((ObjectNode) root).put(FIELDS.FINALHASH.name, Configuration.configuration.globalDigest);
		((ObjectNode) root).put(FIELDS.DIGESTALGO.name, Configuration.configuration.digest.name);
		((ObjectNode) root).put(FIELDS.PROXIFIED.name, Configuration.configuration.isHostProxyfied);
		((ObjectNode) root).put(FIELDS.SEPARATOR.name, SEPARATOR_FIELD);
		logger.debug("Info HostId: "+root.toString());
	}
	
	/**
	 * 
	 * @return the associated HostId
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * 
	 * @return the version for this Host
	 */
	public String getVersion() {
		return root.path(FIELDS.VERSION.name).asText();
	}
	/**
	 * 
	 * @return True if this Host returns FileSize
	 */
	public boolean useFileSize() {
		return root.path(FIELDS.FILESIZE.name).asBoolean((Boolean) FIELDS.FILESIZE.defaultValue);
	}
	/**
	 * 
	 * @return True if this Host returns a final hash
	 */
	public boolean useFinalHash() {
		return root.path(FIELDS.FINALHASH.name).asBoolean((Boolean) FIELDS.FINALHASH.defaultValue);
	}
	/**
	 * 
	 * @return True if this Host returns Digest Algo used
	 */
	public DigestAlgo getDigestAlgo() {
		String algo = root.path(FIELDS.DIGESTALGO.name).asText();
		return getDigestAlgo(algo);
	}
	/**
	 * 
	 * @return True if this Host is proxified
	 */
	public boolean isProxified() {
		return root.path(FIELDS.PROXIFIED.name).asBoolean((Boolean) FIELDS.PROXIFIED.defaultValue);
	}
	/**
	 * 
	 * @return the separator for this Host
	 */
	public String getSeperator() {
		return root.path(FIELDS.SEPARATOR.name).asText();
	}
	/**
	 * 
	 * @return the String representation as version.json
	 */
	public String toString() {
		try {
			return getVersion()+"."+mapper.writeValueAsString(root);
		} catch (JsonProcessingException e) {
			return getVersion();
		}
	}
	
	public static DigestAlgo getDigestAlgo(String algo) {
		for (DigestAlgo alg : DigestAlgo.values()) {
			if (alg.name.equals(algo)) {
				return alg;
			}
		}
		try {
			return DigestAlgo.valueOf(algo);
		} catch (IllegalArgumentException e) {
		}
		return DigestAlgo.MD5;
	}
	/**
	 * 
	 * @param remoteHost
	 * @return the separator to be used
	 */
	public static String getSeparator(String remoteHost) {
		logger.debug("Versions: search: "+remoteHost+ " in {}", Configuration.configuration.versions);
		PartnerConfiguration partner = Configuration.configuration.versions.get(remoteHost);
		if (partner != null) {
			return partner.getSeperator();
		}
		return BLANK_SEPARATOR_FIELD;
	}
	/**
	 * Compare 2 versions
	 * @param version1
	 * @param version2
	 * @return True if version2 >= version1
	 */
	public static boolean isVersion2GEQVersion1(String version1, String version2) {
		if (version1 == null || version2 == null) {
			return false;
		}
		int major1 = 0;
		int rank1 = 0;
		int subversion1 = 0;
		String [] vals = version1.split("\\.");
		major1 = Integer.parseInt(vals[0]);
		rank1 = Integer.parseInt(vals[1]);
		subversion1 = Integer.parseInt(vals[2]);
		int major2 = 0;
		int rank2 = 0;
		int subversion2 = 0;
		vals = version2.split("\\.");
		major2 = Integer.parseInt(vals[0]);
		rank2 = Integer.parseInt(vals[1]);
		subversion2 = Integer.parseInt(vals[2]);
		logger.debug("1: "+major1+":"+rank1+":"+subversion1+" <=? "+major2+":"+rank2+":"+subversion2+ " = "+
				(major1 < major2 || (major1 == major2 && (rank1 < rank2 || (rank1 == rank2 && subversion1 <= subversion2)))));
		return (major1 < major2 || (major1 == major2 && (rank1 < rank2 || (rank1 == rank2 && subversion1 <= subversion2))));
	}
}
