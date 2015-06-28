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
package org.waarp.openr66.context.task;

import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpLogger;
import org.waarp.common.logging.WaarpLoggerFactory;
import org.waarp.common.utility.UUID;

/**
 * Add an UUID in the TransferInformation to the current Task.</br>
 * This should be called on caller side in pre-task since the transfer information will be transfered just after.</br>
 * The last argument is -1 = added in front, +1 = added at last (mandatory).</br>
 * </br>
 * To be called as: <task><type>EXECJAVA</type><path>org.waarp.openr66.context.task.AddUuidJavaTask (-1/+1)</path></task> 
 * 
 * @author "Frederic Bregier"
 *
 */
public class AddUuidJavaTask extends AbstractExecJavaTask {
    /**
     * Internal Logger
     */
    private static final WaarpLogger logger = WaarpLoggerFactory
            .getLogger(AddUuidJavaTask.class);

    @Override
    public void run() {
        UUID uuid = new UUID();
        String way = fullarg.split(" ")[0];
        String fileInfo = null;
        if (way.charAt(0) == '-') {
            fileInfo = "#" + uuid.toString() + "# " + this.session.getRunner().getFileInformation();
        } else {
            fileInfo = this.session.getRunner().getFileInformation() + " #" + uuid.toString() + "#";
        }
        this.session.getRunner().setFileInformation(fileInfo);
        try {
            this.session.getRunner().update();
        } catch (WaarpDatabaseException e) {
            logger.error("UUID cannot be saved to fileInformation:" + fileInfo);
            this.status = 2;
            return;
        }
        logger.debug("UUID saved to fileInformation:" + fileInfo);
        this.status = 0;
    }
}
