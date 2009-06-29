/**
 * Copyright 2009, Frederic Bregier, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors. This is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of the License,
 * or (at your option) any later version. This software is distributed in the
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Lesser General Public License for more details. You should have
 * received a copy of the GNU Lesser General Public License along with this
 * software; if not, write to the Free Software Foundation, Inc., 51 Franklin
 * St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF site:
 * http://www.fsf.org.
 */
package goldengate.r66.core.command;

import goldengate.common.exception.InvalidArgumentException;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.SortedMap;

/**
 * Definition of all Argument of Parameter commands (MODE, STRU, TYPE)
 * 
 * @author Frederic Bregier
 */
public class R66ArgumentCode {

    /**
     * Type of transmission
     * 
     * @author Frederic Bregier
     */
    public static enum TransferType {
        /**
         * Ascii TransferType
         */
        ASCII('A', "ASCII"),
        /**
         * Ebcdic TransferType
         */
        EBCDIC('E', "ebcdic-cp-us"), // could be ebcdic-cp-LG where LG is
        // language like fr, gb, ...
        /**
         * Image TransferType
         */
        IMAGE('I');
        /**
         * TransferType
         */
        public char type;

        /**
         * Charset Name if any
         */
        public String charsetName;

        private TransferType(char type) {
            this.type = type;
            charsetName = null;
        }

        private TransferType(char type, String charsetName) {
            this.type = type;
            this.charsetName = charsetName;
        }
    }

    /**
     * Mode of transmission
     * 
     * @author Frederic Bregier goldengate.ftp.core.data TransferMode
     */
    public static enum TransferMode {
        /**
         * Stream TransferMode
         */
        STREAM('S'),
        /**
         * Block TransferMode
         */
        BLOCK('B'),
        /**
         * Compressed Gzip TransferMode
         */
        COMPRESSED('C');
        /**
         * TransferMode
         */
        public char mode;

        private TransferMode(char mode) {
            this.mode = mode;
        }
    }

    /**
     * Get the TransferType according to the char
     * 
     * @param type
     * @return the corresponding TransferType
     * @exception InvalidArgumentException
     *                if the type is unknown
     */
    public static R66ArgumentCode.TransferType getTransferType(char type)
            throws InvalidArgumentException {
        switch (type) {
        case 'A':
        case 'a':
            return R66ArgumentCode.TransferType.ASCII;
        case 'E':
        case 'e':
            return R66ArgumentCode.TransferType.EBCDIC;
        case 'I':
        case 'i':
            return R66ArgumentCode.TransferType.IMAGE;
        default:
            throw new InvalidArgumentException(
                    "Argument for TransferType is not allowed: " + type);
        }
    }

    /**
     * Get the TransferMode according to the char
     * 
     * @param mode
     * @return the corresponding TransferMode
     * @exception InvalidArgumentException
     *                if the TransferMode is unknown
     */
    public static R66ArgumentCode.TransferMode getTransferMode(char mode)
            throws InvalidArgumentException {
        switch (mode) {
        case 'B':
        case 'b':
            return R66ArgumentCode.TransferMode.BLOCK;
        case 'C':
        case 'c':
            return R66ArgumentCode.TransferMode.COMPRESSED;
        case 'S':
        case 's':
            return R66ArgumentCode.TransferMode.STREAM;
        default:
            throw new InvalidArgumentException(
                    "Argument for TransferMode is not allowed: " + mode);
        }
    }

    /**
     * List all charsets supported by the current platform
     * 
     * @param args
     */
    public static void main(String args[]) {
        final SortedMap<String, Charset> charsets = Charset.availableCharsets();
        final Set<String> names = charsets.keySet();
        for (final String name : names) {
            final Charset charset = charsets.get(name);
            System.out.println(charset);
            final Set<String> aliases = charset.aliases();
            for (final String alias : aliases) {
                System.out.println("   " + alias);
            }
        }
    }
}
