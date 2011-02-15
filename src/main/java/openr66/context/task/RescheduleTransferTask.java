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
package openr66.context.task;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import goldengate.common.database.exception.GoldenGateDatabaseException;
import goldengate.common.logging.GgInternalLogger;
import goldengate.common.logging.GgInternalLoggerFactory;
import openr66.context.ErrorCode;
import openr66.context.R66Result;
import openr66.context.R66Session;
import openr66.context.task.exception.OpenR66RunnerErrorException;
import openr66.database.data.DbTaskRunner;

/**
 * Reschedule Transfer task:<br>
 *
 * Result of arguments will be as following options (the two first are mandatory):<br><br>
 * 
 * "-delay ms" where ms is the added number of ms on current time before retry on schedule<br>
 * "-case errorCode,errorCode,..." where errorCode is one of the following error of the current transfer:<br>
 * ConnectionImpossible, ServerOverloaded, BadAuthent, ExternalOp, TransferError,
 * MD5Error, Disconnection, RemoteShutdown, FinalOp, Unimplemented, Shutdown, RemoteError, 
 * Internal, StoppedTransfer, CanceledTransfer, Warning, Unknown, QueryAlreadyFinished,
 * QueryStillRunning, NotKnownHost, QueryRemotelyUnknown, FileNotFound, CommandNotFound,
 * PassThroughMode<br><br>
 * "-between start;end" and/or "-notbetween start;end" (multiple times are allowed, start or end can be not set)
 * and where start and stop are in the following format:<br>
 * Yn:Mn:Dn:Hn:mn:Sn where n is a number for each time specification, each specification
 *  is optional, as Y=Year, M=Month, D=Day, H=Hour, m=minute, s=second.<br>
 *  If one time specification is not set, it is based on the current date.<br><br>
 *  
 *  Example: -delay 3600000 -case ConnectionImpossible,ServerOverloaded,Shutdown -notbetween H7;H19<br>
 *  means retry in case of error during initialization of connection in 1 hour if not between 7AM to 7PM.<br>
 *
 * @author Frederic Bregier
 *
 */
public class RescheduleTransferTask extends AbstractTask {
    /**
     * Internal Logger
     */
    private static final GgInternalLogger logger = GgInternalLoggerFactory
            .getLogger(RescheduleTransferTask.class);

    protected long newdate = 0;
    protected Calendar newDate = null;

    /**
     * @param argRule
     * @param delay
     * @param argTransfer
     * @param session
     */
    public RescheduleTransferTask(String argRule, int delay, String argTransfer,
            R66Session session) {
        super(TaskType.RESCHEDULE, delay, argRule, argTransfer, session);
    }

    /*
     * (non-Javadoc)
     *
     * @see openr66.context.task.AbstractTask#run()
     */
    @Override
    public void run() {
        logger.info("Reschedule with " + argRule + ":" + argTransfer + " and {}",
                session);
        DbTaskRunner runner = session.getRunner();
        if (runner == null) {
            futureCompletion.setFailure(
                    new OpenR66RunnerErrorException("No valid runner in Reschedule"));
            return;
        }
        String finalname = argRule;
        finalname = getReplacedValue(finalname, argTransfer.split(" "));
        String[] args = finalname.split(" ");
        if (args.length < 4) {
            futureCompletion.setFailure(
                    new OpenR66RunnerErrorException("Not enough argument in Reschedule"));
            return;
        }
        if (!validateArgs(args)) {
            futureCompletion.setFailure(
                    new OpenR66RunnerErrorException("Reschedule unallowed due to argument"));
            return;
        }
        Timestamp start = new Timestamp(newdate);
        try {
            runner.setStart(start);
        } catch (GoldenGateDatabaseException e) {
            futureCompletion.setFailure(
                    new OpenR66RunnerErrorException("Reschedule failed: "+e.getMessage(),e));
            logger.error("Prepare transfer in\n    FAILURE\n     "+runner.toShortString()+
                    "\n    <AT>"+(new Date(newdate)).toString()+"</AT>", e);
            return;
        }
        R66Result result = new R66Result(session, false, ErrorCode.Warning, runner);
        futureCompletion.setResult(result);
        logger.info("Reschedule transfer in\n    SUCCESS\n    "+runner.toShortString()+
                "\n    <AT>"+(new Date(newdate)).toString()+"</AT>");
        futureCompletion.setSuccess();
    }
    
    protected boolean validateArgs(String [] args) {
        boolean validCode = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-delay")) {
                i++;
                try {
                    newdate = Long.parseLong(args[i]);
                } catch (NumberFormatException e) {
                    return false;
                }
            } else if (args[i].equalsIgnoreCase("-case")) {
                i++;
                if (!validCode) {
                    String []codes = args[i].split(",");
                    for (int j = 0; j < codes.length; j++) {
                        if (session.getLocalChannelReference().getCurrentCode() == 
                            ErrorCode.valueOf(codes[j])) {
                            validCode = true;
                        }
                    }
                }
            }
        }
        // now we have new delay plus code
        if (!validCode) {
            return false;
        }
        if (newdate <= 0) {
            return false;
        }
        newdate += System.currentTimeMillis();
        newDate = Calendar.getInstance();
        newDate.setTimeInMillis(newdate);
        boolean betweenTest = false;
        boolean betweenResult = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("-notbetween")) {
                i++;
                String [] elmts = args[i].split(";");
                Calendar start = Calendar.getInstance();
                boolean startModified = false;
                String [] values = elmts[0].split(":");
                for (int j = 0; j < values.length; j++) {
                    if (values[j].length() > 1) {
                        int value;
                        try {
                            value = Integer.parseInt(values[j].substring(1));
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        switch (values[j].charAt(0)) {
                            case 'Y':
                                start.set(Calendar.YEAR, value);
                                startModified = true;
                                break;
                            case 'M':
                                start.set(Calendar.MONTH, value);
                                startModified = true;
                                break;
                            case 'D':
                                start.set(Calendar.DAY_OF_MONTH, value);
                                startModified = true;
                                break;
                            case 'H':
                                start.set(Calendar.HOUR_OF_DAY, value);
                                startModified = true;
                                break;
                            case 'm':
                                start.set(Calendar.MINUTE, value);
                                startModified = true;
                                break;
                            case 'S':
                                start.set(Calendar.SECOND, value);
                                startModified = true;
                                break;
                        }
                    }
                }
                Calendar stop = Calendar.getInstance();
                boolean stopModified = false;
                values = elmts[1].split(":");
                for (int j = 0; j < values.length; j++) {
                    if (values[j].length() > 1) {
                        int value;
                        try {
                            value = Integer.parseInt(values[j].substring(1));
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        switch (values[j].charAt(0)) {
                            case 'Y':
                                stop.set(Calendar.YEAR, value);
                                stopModified = true;
                                break;
                            case 'M':
                                stop.set(Calendar.MONTH, value);
                                stopModified = true;
                                break;
                            case 'D':
                                stop.set(Calendar.DAY_OF_MONTH, value);
                                stopModified = true;
                                break;
                            case 'H':
                                stop.set(Calendar.HOUR_OF_DAY, value);
                                stopModified = true;
                                break;
                            case 'm':
                                stop.set(Calendar.MINUTE, value);
                                stopModified = true;
                                break;
                            case 'S':
                                stop.set(Calendar.SECOND, value);
                                stopModified = true;
                                break;
                        }
                    }
                }
                if (!startModified) {
                    if (newDate.compareTo(stop) < 0)
                        return false;
                } else if (start.compareTo(newDate) < 0) {
                    if ((!stopModified) || (newDate.compareTo(stop) < 0))
                        return false;
                }
            } else if (args[i].equalsIgnoreCase("-between")) {
                i++;
                betweenTest = true;
                String [] elmts = args[i].split(";");
                Calendar start = Calendar.getInstance();
                boolean startModified = false;
                String [] values = elmts[0].split(":");
                for (int j = 0; j < values.length; j++) {
                    if (values[j].length() > 1) {
                        int value;
                        try {
                            value = Integer.parseInt(values[j].substring(1));
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        switch (values[j].charAt(0)) {
                            case 'Y':
                                start.set(Calendar.YEAR, value);
                                startModified = true;
                                break;
                            case 'M':
                                start.set(Calendar.MONTH, value);
                                startModified = true;
                                break;
                            case 'D':
                                start.set(Calendar.DAY_OF_MONTH, value);
                                startModified = true;
                                break;
                            case 'H':
                                start.set(Calendar.HOUR_OF_DAY, value);
                                startModified = true;
                                break;
                            case 'm':
                                start.set(Calendar.MINUTE, value);
                                startModified = true;
                                break;
                            case 'S':
                                start.set(Calendar.SECOND, value);
                                startModified = true;
                                break;
                        }
                    }
                }
                Calendar stop = Calendar.getInstance();
                boolean stopModified = false;
                values = elmts[1].split(":");
                for (int j = 0; j < values.length; j++) {
                    if (values[j].length() > 1) {
                        int value;
                        try {
                            value = Integer.parseInt(values[j].substring(1));
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        switch (values[j].charAt(0)) {
                            case 'Y':
                                stop.set(Calendar.YEAR, value);
                                stopModified = true;
                                break;
                            case 'M':
                                stop.set(Calendar.MONTH, value);
                                stopModified = true;
                                break;
                            case 'D':
                                stop.set(Calendar.DAY_OF_MONTH, value);
                                stopModified = true;
                                break;
                            case 'H':
                                stop.set(Calendar.HOUR_OF_DAY, value);
                                stopModified = true;
                                break;
                            case 'm':
                                stop.set(Calendar.MINUTE, value);
                                stopModified = true;
                                break;
                            case 'S':
                                stop.set(Calendar.SECOND, value);
                                stopModified = true;
                                break;
                        }
                    }
                }
                if (!startModified) {
                    if (newDate.compareTo(stop) < 0)
                        betweenResult = true;
                } else if (start.compareTo(newDate) < 0) {
                    if ((!stopModified) || (newDate.compareTo(stop) < 0))
                        betweenResult = true;
                }
            }
        }
        if (betweenTest)
            return betweenResult;
        return true;
    }

}
