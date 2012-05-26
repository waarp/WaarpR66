/**
   This file is part of GoldenGate Project (named also GoldenGate or GG).

   Copyright 2009, Frederic Bregier, and individual contributors by the @author
   tags. See the COPYRIGHT.txt in the distribution for a full listing of
   individual contributors.

   All GoldenGate Project is free software: you can redistribute it and/or 
   modify it under the terms of the GNU General Public License as published 
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   GoldenGate is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GoldenGate .  If not, see <http://www.gnu.org/licenses/>.
 */
package openr66.protocol.test;

import goldengate.common.command.exception.CommandAbstractException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.context.filesystem.R66File;
import openr66.context.task.AbstractExecJavaTask;
import openr66.protocol.exception.OpenR66ProtocolPacketException;
import openr66.protocol.localhandler.packet.BusinessRequestPacket;
import openr66.protocol.utils.ChannelUtils;

/**
 * @author Frederic Bregier
 *
 */
public class TestExecJavaTask extends AbstractExecJavaTask {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(TestExecJavaTask.class);
    
    @Override
    public void run() {
        if (callFromBusiness) {
            // Business Request to validate?
            if (isToValidate) {
                int rank = Integer.parseInt(args[2]);
                rank++;
                BusinessRequestPacket packet = 
                    new BusinessRequestPacket(this.getClass().getName()+" business "+rank+" final return", 0);
                if (rank > 100) {
                    validate(packet);
                    logger.info("Will NOT close the channel: "+rank);
                } else {
                    logger.info("Continue: "+rank);
                }
                try {
                    ChannelUtils.writeAbstractLocalPacket(session.getLocalChannelReference(),
                            packet).awaitUninterruptibly();
                } catch (OpenR66ProtocolPacketException e) {
                }
                this.status = 0;
                return;
            }
            finalValidate();
            return;
        } else {
            // Rule EXECJAVA based
            R66File file = session.getFile();
            if (file == null) {
                logger.info("TestExecJavaTask No File");
            } else {
                try {
                    logger.info("TestExecJavaTask File: "+file.getFile());
                } catch (CommandAbstractException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            this.status = 0;
        }
    }
}
