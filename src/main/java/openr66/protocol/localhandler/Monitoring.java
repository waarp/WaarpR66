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
package openr66.protocol.localhandler;

import java.util.Date;

import org.jboss.netty.handler.traffic.TrafficCounter;

import goldengate.common.database.DbAdmin;
import goldengate.common.database.DbPreparedStatement;
import goldengate.common.database.DbSession;
import goldengate.common.database.data.AbstractDbData.UpdatedInfo;
import goldengate.common.database.exception.GoldenGateDatabaseNoConnectionError;
import goldengate.common.database.exception.GoldenGateDatabaseSqlError;
import openr66.context.ErrorCode;
import openr66.database.DbConstant;
import openr66.database.data.DbTaskRunner;
import openr66.database.data.DbTaskRunner.TASKSTEP;
import openr66.protocol.configuration.Configuration;

/**
 * Monitoring class as an helper to get values of interest
 * 
 * @author Frederic Bregier
 *
 */
public class Monitoring {
    public long inerrorTransfer = 0;
    public long interruptedTransfer = 0;
    public long inerrorRunning = 0;
    public long toSubmitTransfer = 0;
    public long running = 0;
    public long initOk = 0;
    public long preprocessingOk = 0;
    public long transferOk = 0;
    public long postprocessingOk = 0;
    public long doneTransfer = 0;

    public long nbNetworkConnection = 0;
    public long secondsRunning = 0;
    public long nbThread = 0;
    public long bandwidthIn = 0;
    public long bandwidthOut = 0;
    
    private long startMonitor = System.currentTimeMillis();
    private long pastLimit = 0;
    private long minimalDelay = 0;
    private long lastTry = 0;
    private DbSession dbSession = null;
    private TrafficCounter trafficCounter =
        Configuration.configuration.getGlobalTrafficShapingHandler().getTrafficCounter();

    private DbPreparedStatement countInError;
    private DbPreparedStatement countInterrupted;
    private DbPreparedStatement countInErrorRunning;
    private DbPreparedStatement countToSubmit;
    private DbPreparedStatement countRunningStep;
    private DbPreparedStatement countInitOkStep;
    private DbPreparedStatement countPreProcessingOkStep;
    private DbPreparedStatement countTransferOkStep;
    private DbPreparedStatement countPostProcessingOkStep;
    private DbPreparedStatement countCompleteOkStep;

    
    
    /**
     * @param pastLimit
     * @param minimalDelay
     * @param session
     */
    public Monitoring(long pastLimit, long minimalDelay, DbSession session) {
        this.pastLimit = pastLimit;
        this.minimalDelay = minimalDelay;
        if (session != null) {
            dbSession = session;
        } else {
            dbSession = DbConstant.admin.session;
        }
        this.initialize();
    }

    public void initialize() {
        try {
            countInError = DbTaskRunner.getCountStatsPrepareStatement(
                    dbSession, UpdatedInfo.INERROR);
            countInterrupted = DbTaskRunner.getCountStatsPrepareStatement(
                    dbSession, UpdatedInfo.INTERRUPTED);
            countInErrorRunning = DbTaskRunner.getCountStepPrepareStatement(
                    dbSession, TASKSTEP.ERRORTASK);
            countToSubmit = DbTaskRunner.getCountStatsPrepareStatement(
                    dbSession, UpdatedInfo.TOSUBMIT);
            countRunningStep = DbTaskRunner.getCountStatusPrepareStatement(dbSession,
                    ErrorCode.Running);
            countInitOkStep = DbTaskRunner.getCountStatusPrepareStatement(dbSession,
                    ErrorCode.InitOk);
            countPreProcessingOkStep = DbTaskRunner.getCountStatusPrepareStatement(dbSession,
                    ErrorCode.PreProcessingOk);
            countTransferOkStep = DbTaskRunner.getCountStatusPrepareStatement(dbSession,
                    ErrorCode.TransferOk);
            countPostProcessingOkStep = DbTaskRunner.getCountStatusPrepareStatement(dbSession,
                    ErrorCode.PostProcessingOk);
            countCompleteOkStep = DbTaskRunner.getCountStatusPrepareStatement(dbSession,
                    ErrorCode.CompleteOk);
        } catch (GoldenGateDatabaseNoConnectionError e) {
        } catch (GoldenGateDatabaseSqlError e) {
        }
    }
    
    public void releaseResources() {
        countInError.realClose();
        countInterrupted.realClose();
        countInErrorRunning.realClose();
        countToSubmit.realClose();
        countRunningStep.realClose();
        countInitOkStep.realClose();
        countPreProcessingOkStep.realClose();
        countTransferOkStep.realClose();
        countPostProcessingOkStep.realClose();
        countCompleteOkStep.realClose();
    }
    
    public void run() {
        long limitDate = System.currentTimeMillis();

        nbNetworkConnection = DbAdmin.getNbConnection();
        bandwidthIn = trafficCounter.getLastReadThroughput()>>7;// B/s -> Kb/s
        bandwidthOut = trafficCounter.getLastWriteThroughput()>>7;
        nbThread = Thread.activeCount();
        secondsRunning = (limitDate - startMonitor) / 1000;

        if (limitDate < lastTry+minimalDelay) {
            // too early
            return;
        }
        lastTry = limitDate;
        limitDate -= pastLimit;
        // Update value
        try {
            DbTaskRunner.finishSelectOrCountPrepareStatement(countInError, limitDate);
            inerrorTransfer = DbTaskRunner.getResultCountPrepareStatement(countInError);
            
            DbTaskRunner.finishSelectOrCountPrepareStatement(countInterrupted, limitDate);
            interruptedTransfer = DbTaskRunner.getResultCountPrepareStatement(countInterrupted);
            
            DbTaskRunner.finishSelectOrCountPrepareStatement(countInErrorRunning, limitDate);
            inerrorRunning = DbTaskRunner.getResultCountPrepareStatement(countInErrorRunning);
            
            DbTaskRunner.finishSelectOrCountPrepareStatement(countToSubmit, limitDate);
            toSubmitTransfer = DbTaskRunner.getResultCountPrepareStatement(countToSubmit);

            DbTaskRunner.finishSelectOrCountPrepareStatement(countRunningStep, limitDate);
            running = DbTaskRunner.getResultCountPrepareStatement(countRunningStep);

            DbTaskRunner.finishSelectOrCountPrepareStatement(countInitOkStep, limitDate);
            initOk = DbTaskRunner.getResultCountPrepareStatement(countInitOkStep);

            DbTaskRunner.finishSelectOrCountPrepareStatement(countPreProcessingOkStep, limitDate);
            preprocessingOk = DbTaskRunner.getResultCountPrepareStatement(countPreProcessingOkStep);

            DbTaskRunner.finishSelectOrCountPrepareStatement(countTransferOkStep, limitDate);
            transferOk = DbTaskRunner.getResultCountPrepareStatement(countTransferOkStep);

            DbTaskRunner.finishSelectOrCountPrepareStatement(countPostProcessingOkStep, limitDate);
            postprocessingOk = DbTaskRunner.getResultCountPrepareStatement(countPostProcessingOkStep);

            DbTaskRunner.finishSelectOrCountPrepareStatement(countCompleteOkStep, limitDate);
            doneTransfer = DbTaskRunner.getResultCountPrepareStatement(countCompleteOkStep);
        } catch (GoldenGateDatabaseNoConnectionError e) {
        } catch (GoldenGateDatabaseSqlError e) {
        }
    }
    /**
     * 
     * @return The XML representation of the current status
     */
    public String exportXml() {
        StringBuilder builder = new StringBuilder();
        builder.append("<STATUS>");
        builder.append("<HOSTID>");
        builder.append(Configuration.configuration.HOST_ID);
        builder.append("</HOSTID>");
        builder.append("<DATE>");
        builder.append(new Date());
        builder.append("</DATE>");
        builder.append("<INERRORTRANSFER>");
        builder.append(inerrorTransfer);
        builder.append("</INERRORTRANSFER>");
        builder.append("<INTERRUPTEDTRANSFER>");
        builder.append(interruptedTransfer);
        builder.append("</INTERRUPTEDTRANSFER>");
        builder.append("<INERRORSTEP>");
        builder.append(inerrorRunning);
        builder.append("</INERRORSTEP>");
        builder.append("<TOSUBMITSTEP>");
        builder.append(toSubmitTransfer);
        builder.append("</TOSUBMITSTEP>");
        builder.append("<RUNNING>");
        builder.append(running);
        builder.append("</RUNNING>");
        builder.append("<INITOKSTEP>");
        builder.append(initOk);
        builder.append("</INITOKSTEP>");
        builder.append("<PREPROCESSINGSTEP>");
        builder.append(preprocessingOk);
        builder.append("</PREPROCESSINGSTEP>");
        builder.append("<TRANSFERSTEP>");
        builder.append(transferOk);
        builder.append("</TRANSFERSTEP>");
        builder.append("<POSTPROCESSINGSTEP>");
        builder.append(postprocessingOk);
        builder.append("</POSTPROCESSINGSTEP>");
        builder.append("<COMPLETED>");
        builder.append(doneTransfer);
        builder.append("</COMPLETED>");
        builder.append("<NETWORKCONNECTION>");
        builder.append(nbNetworkConnection);
        builder.append("</NETWORKCONNECTION>");
        builder.append("<SECONDSRUNNING>");
        builder.append(secondsRunning);
        builder.append("</SECONDSRUNNING>");
        builder.append("<NBTHREAD>");
        builder.append(nbThread);
        builder.append("</NBTHREAD>");
        builder.append("<INBANDWIDTH>");
        builder.append(bandwidthIn);
        builder.append("</INBANDWIDTH>");
        builder.append("<OUTBANDWIDTH>");
        builder.append(bandwidthOut);
        builder.append("</OUTBANDWIDTH>");
        builder.append("</STATUS>");
        return builder.toString();
    }
}
