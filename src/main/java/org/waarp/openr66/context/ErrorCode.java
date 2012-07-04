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
package org.waarp.openr66.context;

/**
 * This enum class keeps all code that will be returned into the result and store (char
 * representation) into the runner.
 * 
 * @author Frederic Bregier
 * 
 */
public enum ErrorCode {
	/**
	 * Code stands for initialization ok (internal connection, authentication)
	 */
	InitOk("Initialization step ok", 'i'),
	/**
	 * Code stands for pre processing ok
	 */
	PreProcessingOk(
			"PreProcessing step ok",
			'B'),
	/**
	 * Code stands for transfer OK
	 */
	TransferOk("Transfer step ok", 'X'),
	/**
	 * Code stands for post processing ok
	 */
	PostProcessingOk(
			"PostProcessing step ok",
			'P'),
	/**
	 * Code stands for All action are completed ok
	 */
	CompleteOk("Operation completed", 'O'),
	/**
	 * Code stands for connection is impossible (remote or local reason)
	 */
	ConnectionImpossible(
			"Connection Impossible",
			'C'),
	/**
	 * Code stands for connection is impossible now due to limits(remote or local reason)
	 */
	ServerOverloaded(
			"Connection delayed due to exceed of capacity",
			'l'),
	/**
	 * Code stands for bad authentication (remote or local)
	 */
	BadAuthent("Bad Authentication", 'A'),
	/**
	 * Code stands for External operation in error (pre, post or error processing)
	 */
	ExternalOp(
			"External Operation as Task in error",
			'E'),
	/**
	 * Code stands for Transfer is in error
	 */
	TransferError("Bad Transfer", 'T'),
	/**
	 * Code stands for Transfer in error due to MD5
	 */
	MD5Error(
			"MD5 during transfer in error",
			'M'),
	/**
	 * Code stands for Network disconnection
	 */
	Disconnection("Disconnection before end", 'D'),
	/**
	 * Code stands for Remote Shutdown
	 */
	RemoteShutdown("Disconnection before end due to a remote shutdown", 'r'),
	/**
	 * Code stands for final action (like moving file) is in error
	 */
	FinalOp(
			"Final Operation on the result file in error",
			'F'),
	/**
	 * Code stands for unimplemented feature
	 */
	Unimplemented("Function not implemented", 'U'),
	/**
	 * Code stands for shutdown is in progress
	 */
	Shutdown(
			"Shutdown order",
			'S'),
	/**
	 * Code stands for a remote error is received
	 */
	RemoteError("Error due to remote", 'R'),
	/**
	 * Code stands for an internal error
	 */
	Internal(
			"Internal Error",
			'I'),
	/**
	 * Code stands for a request of stopping transfer
	 */
	StoppedTransfer("Stopped Transfer", 'H'),
	/**
	 * Code stands for a request of canceling transfer
	 */
	CanceledTransfer("Canceled Transfer", 'K'),
	/**
	 * Warning in execution
	 */
	Warning("Warning during pre or post execution", 'W'),
	/**
	 * Code stands for unknown type of error
	 */
	Unknown("Unknown status", '-'),
	/**
	 * Code stands for a request that is already remotely finished
	 */
	QueryAlreadyFinished("Restart Query for a transfer already finished", 'Q'),
	/**
	 * Code stands for request that is still running
	 */
	QueryStillRunning("Restart Query for a transfer still running", 's'),
	/**
	 * Code stands for not known host
	 */
	NotKnownHost("Not known remote host", 'N'),
	/**
	 * Code stands for self requested host starting request is invalid
	 */
	LoopSelfRequestedHost("Host tries to start a self requested transfer", 'N'),
	/**
	 * Code stands for request should exist but is not found on remote host
	 */
	QueryRemotelyUnknown("Not known remote asked query", 'u'),
	/**
	 * Code stands for File not found error
	 */
	FileNotFound("File not found", 'f'),
	/**
	 * Code stands for Command not found error
	 */
	CommandNotFound("Command not found", 'c'),
	/**
	 * Code stands for a request in PassThroughMode and required action is incompatible with this
	 * mode
	 */
	PassThroughMode("Error since action cannot be taken on PassThroughMode", 'p'),
	/**
	 * Code stands for running step
	 */
	Running("Current step in running", 'z');

	/**
	 * Literal for this code
	 */
	public String	mesg;
	/**
	 * Code could be used to switch case operations
	 */
	public char		code;

	private ErrorCode(String mesg, char code) {
		this.mesg = mesg;
		this.code = code;
	}

	public String getCode() {
		return String.valueOf(code);
	}

	/**
	 * Code is either the 1 char code or the exact name in Enum
	 * 
	 * @param code
	 * @return the ErrorCode according to the code
	 */
	public static ErrorCode getFromCode(String code) {
		switch (code.charAt(0)) {
			case 'i':
				return InitOk;
			case 'B':
				return PreProcessingOk;
			case 'P':
				return PostProcessingOk;
			case 'X':
				return TransferOk;
			case 'O':
				return CompleteOk;
			case 'C':
				return ConnectionImpossible;
			case 'A':
				return BadAuthent;
			case 'E':
				return ExternalOp;
			case 'T':
				return TransferError;
			case 'M':
				return MD5Error;
			case 'D':
				return Disconnection;
			case 'r':
				return RemoteShutdown;
			case 'F':
				return FinalOp;
			case 'U':
				return Unimplemented;
			case 'S':
				return Shutdown;
			case 'R':
				return RemoteError;
			case 'I':
				return Internal;
			case 'H':
				return StoppedTransfer;
			case 'K':
				return CanceledTransfer;
			case 'W':
				return Warning;
			case '-':
				return Unknown;
			case 'Q':
				return QueryAlreadyFinished;
			case 's':
				return QueryStillRunning;
			case 'N':
				return NotKnownHost;
			case 'L':
				return LoopSelfRequestedHost;
			case 'u':
				return QueryRemotelyUnknown;
			case 'f':
				return FileNotFound;
			case 'z':
				return Running;
			case 'c':
				return CommandNotFound;
			case 'p':
				return PassThroughMode;
			case 'l':
				return ServerOverloaded;
			default:
				ErrorCode ecode = Unknown;
				try {
					ecode = ErrorCode.valueOf(code.trim());
				} catch (IllegalArgumentException e) {
					return Unknown;
				}
				return ecode;
		}
	}
}
