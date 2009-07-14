/**
 * Copyright 2009, Frederic Bregier, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package openr66.protocol.test;

import java.io.File;

import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import goldengate.common.logging.GgSlf4JLoggerFactory;

import openr66.protocol.exception.OpenR66ProtocolSystemException;
import openr66.protocol.utils.FileUtils;

import org.jboss.netty.logging.InternalLoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * @author Frederic Bregier
 *
 */
public class TestCopyFile {

    /**
     * @param args
     */
    public static void main(String[] args) {
        InternalLoggerFactory.setDefaultFactory(new GgSlf4JLoggerFactory(
                Level.WARN));
        final GgInternalLogger logger = GgInternalLoggerFactory
                .getLogger(TestClientShutdown.class);
        if (args.length < 3) {
            logger.warn("Need 2 files and 1 to copy or n > 1 to append n times");
            return;
        }
        File from = new File(args[0]);
        File to = new File(args[1]);
        int nb = Integer.parseInt(args[2]);
        if (! from.canRead()) {
            logger.warn("First arg must be a readable file: "+from.getAbsolutePath());
            return;
        }
        if (to.exists()) {
            to.delete();
        }
        if (nb <= 1) {
            try {
                FileUtils.copy(from, to, false, false);
            } catch (OpenR66ProtocolSystemException e) {
                logger.warn("Copy unsuccessfull",e);
                return;
            }
            System.out.println("Result: "+(from.length() == to.length()));
        } else {
            for (int i = 0; i < nb ; i++) {
                try {
                    FileUtils.copy(from, to, false, true);
                } catch (OpenR66ProtocolSystemException e) {
                    logger.warn("Append unsuccessfull at iteration "+i,e);
                    return;
                }
            }
            System.out.println("Result: "+(nb*from.length() == to.length()));
        }
    }

}
