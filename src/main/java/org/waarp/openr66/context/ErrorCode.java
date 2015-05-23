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

import org.waarp.openr66.protocol.configuration.Messages;

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
    InitOk(Messages.getString("ErrorCode.0"), 'i'), //$NON-NLS-1$
    /**
     * Code stands for pre processing ok
     */
    PreProcessingOk(
            Messages.getString("ErrorCode.1"), //$NON-NLS-1$
            'B'),
    /**
     * Code stands for transfer OK
     */
    TransferOk(Messages.getString("ErrorCode.2"), 'X'), //$NON-NLS-1$
    /**
     * Code stands for post processing ok
     */
    PostProcessingOk(
            Messages.getString("ErrorCode.3"), //$NON-NLS-1$
            'P'),
    /**
     * Code stands for All action are completed ok
     */
    CompleteOk(Messages.getString("ErrorCode.4"), 'O'), //$NON-NLS-1$
    /**
     * Code stands for connection is impossible (remote or local reason)
     */
    ConnectionImpossible(
            Messages.getString("ErrorCode.5"), //$NON-NLS-1$
            'C'),
    /**
     * Code stands for connection is impossible now due to limits(remote or local reason)
     */
    ServerOverloaded(
            Messages.getString("ErrorCode.6"), //$NON-NLS-1$
            'l'),
    /**
     * Code stands for bad authentication (remote or local)
     */
    BadAuthent(Messages.getString("ErrorCode.7"), 'A'), //$NON-NLS-1$
    /**
     * Code stands for External operation in error (pre, post or error processing)
     */
    ExternalOp(
            Messages.getString("ErrorCode.8"), //$NON-NLS-1$
            'E'),
    /**
     * Code stands for Transfer is in error
     */
    TransferError(Messages.getString("ErrorCode.9"), 'T'), //$NON-NLS-1$
    /**
     * Code stands for Transfer in error due to MD5
     */
    MD5Error(
            Messages.getString("ErrorCode.10"), //$NON-NLS-1$
            'M'),
    /**
     * Code stands for Network disconnection
     */
    Disconnection(Messages.getString("ErrorCode.11"), 'D'), //$NON-NLS-1$
    /**
     * Code stands for Remote Shutdown
     */
    RemoteShutdown(Messages.getString("ErrorCode.12"), 'r'), //$NON-NLS-1$
    /**
     * Code stands for final action (like moving file) is in error
     */
    FinalOp(
            Messages.getString("ErrorCode.13"), //$NON-NLS-1$
            'F'),
    /**
     * Code stands for unimplemented feature
     */
    Unimplemented(Messages.getString("ErrorCode.14"), 'U'), //$NON-NLS-1$
    /**
     * Code stands for shutdown is in progress
     */
    Shutdown(
            Messages.getString("ErrorCode.15"), //$NON-NLS-1$
            'S'),
    /**
     * Code stands for a remote error is received
     */
    RemoteError(Messages.getString("ErrorCode.16"), 'R'), //$NON-NLS-1$
    /**
     * Code stands for an internal error
     */
    Internal(
            Messages.getString("ErrorCode.17"), //$NON-NLS-1$
            'I'),
    /**
     * Code stands for a request of stopping transfer
     */
    StoppedTransfer(Messages.getString("ErrorCode.18"), 'H'), //$NON-NLS-1$
    /**
     * Code stands for a request of canceling transfer
     */
    CanceledTransfer(Messages.getString("ErrorCode.19"), 'K'), //$NON-NLS-1$
    /**
     * Warning in execution
     */
    Warning(Messages.getString("ErrorCode.20"), 'W'), //$NON-NLS-1$
    /**
     * Code stands for unknown type of error
     */
    Unknown(Messages.getString("ErrorCode.21"), '-'), //$NON-NLS-1$
    /**
     * Code stands for a request that is already remotely finished
     */
    QueryAlreadyFinished(Messages.getString("ErrorCode.22"), 'Q'), //$NON-NLS-1$
    /**
     * Code stands for request that is still running
     */
    QueryStillRunning(Messages.getString("ErrorCode.23"), 's'), //$NON-NLS-1$
    /**
     * Code stands for not known host
     */
    NotKnownHost(Messages.getString("ErrorCode.24"), 'N'), //$NON-NLS-1$
    /**
     * Code stands for self requested host starting request is invalid
     */
    LoopSelfRequestedHost(Messages.getString("ErrorCode.25"), 'L'), //$NON-NLS-1$
    /**
     * Code stands for request should exist but is not found on remote host
     */
    QueryRemotelyUnknown(Messages.getString("ErrorCode.26"), 'u'), //$NON-NLS-1$
    /**
     * Code stands for File not found error
     */
    FileNotFound(Messages.getString("ErrorCode.27"), 'f'), //$NON-NLS-1$
    /**
     * Code stands for Command not found error
     */
    CommandNotFound(Messages.getString("ErrorCode.28"), 'c'), //$NON-NLS-1$
    /**
     * Code stands for a request in PassThroughMode and required action is incompatible with this
     * mode
     */
    PassThroughMode(Messages.getString("ErrorCode.29"), 'p'), //$NON-NLS-1$
    /**
     * Code stands for running step
     */
    Running(Messages.getString("ErrorCode.30"), 'z'), //$NON-NLS-1$
    /**
     * Code stands for Incorrect command
     */
    IncorrectCommand(Messages.getString("ErrorCode.31"), 'n'), //$NON-NLS-1$
    /**
     * Code stands for File not allowed
     */
    FileNotAllowed(Messages.getString("ErrorCode.32"), 'a'), //$NON-NLS-1$
    /**
     * Code stands for Size not allowed
     */
    SizeNotAllowed(Messages.getString("ErrorCode.33"), 'd'); //$NON-NLS-1$

    /**
     * Literal for this code
     */
    public String mesg;
    /**
     * Code could be used to switch case operations
     */
    public char code;

    private ErrorCode(String mesg, char code) {
        this.mesg = mesg;
        this.code = code;
    }

    public String getCode() {
        return String.valueOf(code);
    }

    /**
     * Update messages from current Language
     */
    public static void updateLang() {
        InitOk.mesg = Messages.getString("ErrorCode.0");
        PreProcessingOk.mesg = Messages.getString("ErrorCode.1");
        TransferOk.mesg = Messages.getString("ErrorCode.2");
        PostProcessingOk.mesg = Messages.getString("ErrorCode.3");
        CompleteOk.mesg = Messages.getString("ErrorCode.4");
        ConnectionImpossible.mesg = Messages.getString("ErrorCode.5");
        ServerOverloaded.mesg = Messages.getString("ErrorCode.6");
        BadAuthent.mesg = Messages.getString("ErrorCode.7");
        ExternalOp.mesg = Messages.getString("ErrorCode.8");
        TransferError.mesg = Messages.getString("ErrorCode.9");
        MD5Error.mesg = Messages.getString("ErrorCode.10");
        Disconnection.mesg = Messages.getString("ErrorCode.11");
        RemoteShutdown.mesg = Messages.getString("ErrorCode.12");
        FinalOp.mesg = Messages.getString("ErrorCode.13");
        Unimplemented.mesg = Messages.getString("ErrorCode.14");
        Shutdown.mesg = Messages.getString("ErrorCode.15");
        RemoteError.mesg = Messages.getString("ErrorCode.16");
        Internal.mesg = Messages.getString("ErrorCode.17");
        StoppedTransfer.mesg = Messages.getString("ErrorCode.18");
        CanceledTransfer.mesg = Messages.getString("ErrorCode.19");
        Warning.mesg = Messages.getString("ErrorCode.20");
        Unknown.mesg = Messages.getString("ErrorCode.21");
        QueryAlreadyFinished.mesg = Messages.getString("ErrorCode.22");
        QueryStillRunning.mesg = Messages.getString("ErrorCode.23");
        NotKnownHost.mesg = Messages.getString("ErrorCode.24");
        LoopSelfRequestedHost.mesg = Messages.getString("ErrorCode.25");
        QueryRemotelyUnknown.mesg = Messages.getString("ErrorCode.26");
        FileNotFound.mesg = Messages.getString("ErrorCode.27");
        CommandNotFound.mesg = Messages.getString("ErrorCode.28");
        PassThroughMode.mesg = Messages.getString("ErrorCode.29");
        Running.mesg = Messages.getString("ErrorCode.30");
        IncorrectCommand.mesg = Messages.getString("ErrorCode.31");
        FileNotAllowed.mesg = Messages.getString("ErrorCode.32");
        SizeNotAllowed.mesg = Messages.getString("ErrorCode.33");
    }

    /**
     * Code is either the 1 char code or the exact name in Enum
     * 
     * @param code
     * @return the ErrorCode according to the code
     */
    public static ErrorCode getFromCode(String code) {
        if (code.isEmpty()) {
            return Unknown;
        }
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
            case 'n':
                return IncorrectCommand;
            case 'a':
                return FileNotAllowed;
            case 'd':
                return SizeNotAllowed;
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

    public static boolean isErrorCode(ErrorCode code) {
        switch (code) {
            case BadAuthent:
            case CanceledTransfer:
            case CommandNotFound:
            case ConnectionImpossible:
            case Disconnection:
            case ExternalOp:
            case FileNotFound:
            case FinalOp:
            case Internal:
            case LoopSelfRequestedHost:
            case MD5Error:
            case NotKnownHost:
            case PassThroughMode:
            case QueryAlreadyFinished:
            case QueryRemotelyUnknown:
            case QueryStillRunning:
            case RemoteError:
            case RemoteShutdown:
            case ServerOverloaded:
            case Shutdown:
            case StoppedTransfer:
            case TransferError:
            case Unimplemented:
            case IncorrectCommand:
            case FileNotAllowed:
            case SizeNotAllowed:
                return true;
            case CompleteOk:
            case InitOk:
            case PostProcessingOk:
            case PreProcessingOk:
            case Running:
            case TransferOk:
            case Unknown:
            case Warning:
                return false;
            default:
                break;
        }
        return true;
    }
}
