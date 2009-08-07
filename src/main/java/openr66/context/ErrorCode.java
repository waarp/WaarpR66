/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package openr66.context;

/**
 * This enum class keeps all code that will be returned into the result
 * and store (char representation) into the runner.
 *
 * @author Frederic Bregier
 *
 */
public enum ErrorCode {
    /**
     * Code stands for initialization ok (internal connection, authentication)
     */
    InitOk("Initialization step", 'i'),
    /**
     * Code stands for pre processing ok
     */
    PreProcessingOk(
            "PreProcessing step",
            'B'),
    /**
     * Code stands for transfer OK
     */
    TransferOk("Transfer step", 'X'),
    /**
     * Code stands for post processing ok
     */
    PostProcessingOk(
            "PostProcessing step",
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
    CanceledTransfer("Canceled Transfer", 'Q'),
    /**
     * Warning in execution
     */
    Warning("Warning during pre or post execution", 'W'),
    /**
     * Code stands for unknown type of error
     */
    Unknown("Unknown type of error", '-'),
    /**
     * Code stands for unknown type of error
     */
    Running("Current step in running", 'z');

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
     *
     * @param code
     * @return the ErrorCode according to the code
     */
    public static ErrorCode getFromCode(String code) {
        // FIXME if code could longer than 1 char, make an efficient function to retrieve the status
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
            case 'Q':
                return CanceledTransfer;
            case 'W':
                return Warning;
            case '-':
                return Unknown;
            case 'z':
                return Running;
            default:
                return Unknown;

        }
    }
}
