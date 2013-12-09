package org.waarp.openr66.protocol.utils;

public enum R66Versions {
	/**
	 * Not usable for extra information
	 */
	V2_4_12, 
	/**
	 * Introducing different separator, adding HostConfiguration table
	 */
	V2_4_13, 
	/**
	 * Add TransferInformation to TaskRunner table
	 */
	V2_4_17,
	/**
	 * Add IsActive on DbHostAuth table
	 */
	V2_4_23;
	
	public String getVersion() {
		return this.name().substring(1).replace('_', '.');
	}
}