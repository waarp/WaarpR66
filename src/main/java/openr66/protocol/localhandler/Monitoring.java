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
    // global informations
    public long nbNetworkConnection = 0;
    public long secondsRunning = 0;
    public long nbThread = 0;
    public long bandwidthIn = 0;
    public long bandwidthOut = 0;
    
    // Internal data
    private long startMonitor = System.currentTimeMillis();
    private long pastLimit = 0;
    private long minimalDelay = 0;
    private long lastTry = 0;
    private DbSession dbSession = null;
    private TrafficCounter trafficCounter =
        Configuration.configuration.getGlobalTrafficShapingHandler().getTrafficCounter();

    // Overall status including past, future and current transfers
    private DbPreparedStatement countInfo = null;

    // Current situation of all transfers, running or not
    private DbPreparedStatement countStepAllTransfer = null;
    private DbPreparedStatement countStepNotask = null;
    private DbPreparedStatement countStepPretask = null;
    private DbPreparedStatement countStepTransfer = null;
    private DbPreparedStatement countStepPosttask = null;
    private DbPreparedStatement countStepAllDone = null;
    private DbPreparedStatement countStepError = null;
    
    // First on Running Transfers only
    private DbPreparedStatement countAllRunningStep = null;
    private DbPreparedStatement countRunningStep = null;
    private DbPreparedStatement countInitOkStep = null;
    private DbPreparedStatement countPreProcessingOkStep = null;
    private DbPreparedStatement countTransferOkStep = null;
    private DbPreparedStatement countPostProcessingOkStep = null;
    private DbPreparedStatement countCompleteOkStep = null;
    
    // Error Status on all transfers
    private DbPreparedStatement countStatus = null;

    // Overall status including past, future and current transfers
    public long nbCountInfoUnknown = 0;
    public long nbCountInfoNotUpdated = 0;
    public long nbCountInfoInterrupted = 0;
    public long nbCountInfoToSubmit = 0;
    public long nbCountInfoError = 0;
    public long nbCountInfoRunning = 0;
    public long nbCountInfoDone = 0;

    // Current situation of all transfers, running or not
    public long nbCountStepAllTransfer = 0;
    public long nbCountStepNotask = 0;
    public long nbCountStepPretask = 0;
    public long nbCountStepTransfer = 0;
    public long nbCountStepPosttask = 0;
    public long nbCountStepAllDone = 0;
    public long nbCountStepError = 0;
    
    // First on Running Transfers only
    public long nbCountAllRunningStep = 0;
    public long nbCountRunningStep = 0;
    public long nbCountInitOkStep = 0;
    public long nbCountPreProcessingOkStep = 0;
    public long nbCountTransferOkStep = 0;
    public long nbCountPostProcessingOkStep = 0;
    public long nbCountCompleteOkStep = 0;
    
    // Error Status on all transfers
    public long nbCountStatusConnectionImpossible = 0;
    public long nbCountStatusServerOverloaded = 0;
    public long nbCountStatusBadAuthent = 0;
    public long nbCountStatusExternalOp = 0;
    public long nbCountStatusTransferError = 0;
    public long nbCountStatusMD5Error = 0;
    public long nbCountStatusDisconnection = 0;
    public long nbCountStatusFinalOp = 0;
    public long nbCountStatusUnimplemented = 0;
    public long nbCountStatusInternal = 0;
    public long nbCountStatusWarning = 0;
    public long nbCountStatusQueryAlreadyFinished = 0;
    public long nbCountStatusQueryStillRunning = 0;
    public long nbCountStatusNotKnownHost = 0;
    public long nbCountStatusQueryRemotelyUnknown = 0;
    public long nbCountStatusCommandNotFound = 0;
    public long nbCountStatusPassThroughMode = 0;
    public long nbCountStatusRemoteShutdown = 0;
    public long nbCountStatusShutdown = 0;
    public long nbCountStatusRemoteError = 0;
    public long nbCountStatusStopped = 0;
    public long nbCountStatusCanceled = 0;
    public long nbCountStatusFileNotFound = 0;
    public long nbCountStatusUnknown = 0;
    
    
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
    /**
     * Initialize the Db Requests after constructor or after use of releaseResources
     */
    public void initialize() {
        try {
            // Overall status including past, future and current transfers
            countInfo = DbTaskRunner.getCountInfoPrepareStatement(dbSession);

            // Current situation of all transfers, running or not
            countStepAllTransfer = DbTaskRunner.getCountStepPrepareStatement(
                    dbSession, null);
            countStepNotask = DbTaskRunner.getCountStepPrepareStatement(
                    dbSession, TASKSTEP.NOTASK);
            countStepPretask = DbTaskRunner.getCountStepPrepareStatement(
                    dbSession, TASKSTEP.PRETASK);
            countStepTransfer = DbTaskRunner.getCountStepPrepareStatement(
                    dbSession, TASKSTEP.TRANSFERTASK);
            countStepPosttask = DbTaskRunner.getCountStepPrepareStatement(
                    dbSession, TASKSTEP.POSTTASK);
            countStepAllDone = DbTaskRunner.getCountStepPrepareStatement(
                    dbSession, TASKSTEP.ALLDONETASK);
            countStepError = DbTaskRunner.getCountStepPrepareStatement(
                    dbSession, TASKSTEP.ERRORTASK);
            
            // First on Running Transfers only
            countAllRunningStep = DbTaskRunner.getCountStatusRunningPrepareStatement(dbSession,
                    null);
            countRunningStep = DbTaskRunner.getCountStatusRunningPrepareStatement(dbSession,
                    ErrorCode.Running);
            countInitOkStep = DbTaskRunner.getCountStatusRunningPrepareStatement(dbSession,
                    ErrorCode.InitOk);
            countPreProcessingOkStep = DbTaskRunner.getCountStatusRunningPrepareStatement(dbSession,
                    ErrorCode.PreProcessingOk);
            countTransferOkStep = DbTaskRunner.getCountStatusRunningPrepareStatement(dbSession,
                    ErrorCode.TransferOk);
            countPostProcessingOkStep = DbTaskRunner.getCountStatusRunningPrepareStatement(dbSession,
                    ErrorCode.PostProcessingOk);
            countCompleteOkStep = DbTaskRunner.getCountStatusRunningPrepareStatement(dbSession,
                    ErrorCode.CompleteOk);
            
            // Error Status on all transfers
            countStatus = DbTaskRunner.getCountStatusPrepareStatement(dbSession);
        } catch (GoldenGateDatabaseNoConnectionError e) {
        } catch (GoldenGateDatabaseSqlError e) {
        }
    }
    /**
     * Release all Db Requests
     */
    public void releaseResources() {
        try {
            // Overall status including past, future and current transfers
            countInfo.realClose();
    
            // Current situation of all transfers, running or not
            countStepAllTransfer.realClose();
            countStepNotask.realClose();
            countStepPretask.realClose();
            countStepTransfer.realClose();
            countStepPosttask.realClose();
            countStepAllDone.realClose();
            countStepError.realClose();
            
            // First on Running Transfers only
            countAllRunningStep.realClose();
            countRunningStep.realClose();
            countInitOkStep.realClose();
            countPreProcessingOkStep.realClose();
            countTransferOkStep.realClose();
            countPostProcessingOkStep.realClose();
            countCompleteOkStep.realClose();
            
            // Error Status on all transfers
            countStatus.realClose();
        } catch (NullPointerException e) {
        }
    }
    /**
     * 
     * @return the last Time in ms of the execution
     */
    public long lastRunTimeMs() {
        return lastTry;
    }
    /**
     * Default execution of testing with default pastLimit
     */
    public void run() {
        this.run(-1, false);
    }
    /**
     * 
     * @param nbSecond as specific PastLimit
     */
    public void run(int nbSecond) {
        this.run(nbSecond, false);
    }
    /**
     * Default execution of testing with default pastLimit
     * 
     * @param detail as to get detailed information
     */
    public void run(boolean detail) {
        this.run(-1, detail);
    }
    /**
     * 
     * @param nbSecond as specific PastLimit
     * @param detail as to get detailed information
     */
    public void run(int nbSecond, boolean detail) {
        long limitDate = System.currentTimeMillis();
        long nbMs = pastLimit;
        if (nbSecond <= 0) {
            nbMs = pastLimit;
        } else {
            nbMs = nbSecond*1000;
        }
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
        limitDate -= nbMs;
        // Update value
        try {
            // Overall status including past, future and current transfers
            nbCountInfoUnknown = DbTaskRunner.getResultCountPrepareStatement(countInfo, 
                    UpdatedInfo.UNKNOWN, limitDate);
            nbCountInfoNotUpdated = DbTaskRunner.getResultCountPrepareStatement(countInfo, 
                    UpdatedInfo.NOTUPDATED, limitDate);
            nbCountInfoInterrupted = DbTaskRunner.getResultCountPrepareStatement(countInfo, 
                    UpdatedInfo.INTERRUPTED, limitDate);
            nbCountInfoToSubmit = DbTaskRunner.getResultCountPrepareStatement(countInfo, 
                    UpdatedInfo.TOSUBMIT, limitDate);
            nbCountInfoError = DbTaskRunner.getResultCountPrepareStatement(countInfo, 
                    UpdatedInfo.INERROR, limitDate);
            nbCountInfoRunning = DbTaskRunner.getResultCountPrepareStatement(countInfo, 
                    UpdatedInfo.RUNNING, limitDate);
            nbCountInfoDone = DbTaskRunner.getResultCountPrepareStatement(countInfo, 
                    UpdatedInfo.DONE, limitDate);
    
            // Current situation of all transfers, running or not
            DbTaskRunner.finishSelectOrCountPrepareStatement(countStepAllTransfer, limitDate);
            nbCountStepAllTransfer = DbTaskRunner.getResultCountPrepareStatement(countStepAllTransfer);
            DbTaskRunner.finishSelectOrCountPrepareStatement(countStepNotask, limitDate);
            nbCountStepNotask = DbTaskRunner.getResultCountPrepareStatement(countStepNotask);
            DbTaskRunner.finishSelectOrCountPrepareStatement(countStepPretask, limitDate);
            nbCountStepPretask = DbTaskRunner.getResultCountPrepareStatement(countStepPretask);
            DbTaskRunner.finishSelectOrCountPrepareStatement(countStepTransfer, limitDate);
            nbCountStepTransfer = DbTaskRunner.getResultCountPrepareStatement(countStepTransfer);
            DbTaskRunner.finishSelectOrCountPrepareStatement(countStepPosttask, limitDate);
            nbCountStepPosttask = DbTaskRunner.getResultCountPrepareStatement(countStepPosttask);
            DbTaskRunner.finishSelectOrCountPrepareStatement(countStepAllDone, limitDate);
            nbCountStepAllDone = DbTaskRunner.getResultCountPrepareStatement(countStepAllDone);
            DbTaskRunner.finishSelectOrCountPrepareStatement(countStepError, limitDate);
            nbCountStepError = DbTaskRunner.getResultCountPrepareStatement(countStepError);
            
            DbTaskRunner.finishSelectOrCountPrepareStatement(countAllRunningStep, limitDate);
            nbCountAllRunningStep = DbTaskRunner.getResultCountPrepareStatement(countAllRunningStep);

            if (detail) {
                // First on Running Transfers only
                DbTaskRunner.finishSelectOrCountPrepareStatement(countRunningStep, limitDate);
                nbCountRunningStep = DbTaskRunner.getResultCountPrepareStatement(countRunningStep);
                DbTaskRunner.finishSelectOrCountPrepareStatement(countInitOkStep, limitDate);
                nbCountInitOkStep = DbTaskRunner.getResultCountPrepareStatement(countInitOkStep);
                DbTaskRunner.finishSelectOrCountPrepareStatement(countPreProcessingOkStep, limitDate);
                nbCountPreProcessingOkStep = DbTaskRunner.getResultCountPrepareStatement(countPreProcessingOkStep);
                DbTaskRunner.finishSelectOrCountPrepareStatement(countTransferOkStep, limitDate);
                nbCountTransferOkStep = DbTaskRunner.getResultCountPrepareStatement(countTransferOkStep);
                DbTaskRunner.finishSelectOrCountPrepareStatement(countPostProcessingOkStep, limitDate);
                nbCountPostProcessingOkStep = DbTaskRunner.getResultCountPrepareStatement(countPostProcessingOkStep);
                DbTaskRunner.finishSelectOrCountPrepareStatement(countCompleteOkStep, limitDate);
                nbCountCompleteOkStep = DbTaskRunner.getResultCountPrepareStatement(countCompleteOkStep);
                
                // Error Status on all transfers
                nbCountStatusConnectionImpossible = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.ConnectionImpossible, limitDate);
                nbCountStatusServerOverloaded = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.ServerOverloaded, limitDate);
                nbCountStatusBadAuthent = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.BadAuthent, limitDate);
                nbCountStatusExternalOp = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.ExternalOp, limitDate);
                nbCountStatusTransferError = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.TransferError, limitDate);
                nbCountStatusMD5Error = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.MD5Error, limitDate);
                nbCountStatusDisconnection = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.Disconnection, limitDate);
                nbCountStatusFinalOp = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.FinalOp, limitDate);
                nbCountStatusUnimplemented = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.Unimplemented, limitDate);
                nbCountStatusInternal = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.Internal, limitDate);
                nbCountStatusWarning = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.Warning, limitDate);
                nbCountStatusQueryAlreadyFinished = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.QueryAlreadyFinished, limitDate);
                nbCountStatusQueryStillRunning = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.QueryStillRunning, limitDate);
                nbCountStatusNotKnownHost = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.NotKnownHost, limitDate);
                nbCountStatusQueryRemotelyUnknown = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.QueryRemotelyUnknown, limitDate);
                nbCountStatusCommandNotFound = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.CommandNotFound, limitDate);
                nbCountStatusPassThroughMode = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.PassThroughMode, limitDate);
                nbCountStatusRemoteShutdown = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.RemoteShutdown, limitDate);
                nbCountStatusShutdown = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.Shutdown, limitDate);
                nbCountStatusRemoteError = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.RemoteError, limitDate);
                nbCountStatusStopped = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.StoppedTransfer, limitDate);
                nbCountStatusCanceled = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.CanceledTransfer, limitDate);
                nbCountStatusFileNotFound = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.FileNotFound, limitDate);
                nbCountStatusUnknown = DbTaskRunner.getResultCountPrepareStatement(countStatus,
                        ErrorCode.Unknown, limitDate);
            }
        } catch (GoldenGateDatabaseNoConnectionError e) {
        } catch (GoldenGateDatabaseSqlError e) {
        }
    }
    /**
     * @param detail
     * @return The XML representation of the current status
     */
    public String exportXml(boolean detail) {
        StringBuilder builder = new StringBuilder();
        builder.append("<STATUS>");
        // Global Informations
        builder.append("<HostID>");
        builder.append(Configuration.configuration.HOST_ID);
        builder.append("</HostID>");
        builder.append("<Date>");
        builder.append(new Date());
        builder.append("</Date>");
        builder.append("<LastRun>");
        builder.append(new Date(lastTry));
        builder.append("</LastRun>");
        builder.append("<SecondsRunning>");
        builder.append(secondsRunning);
        builder.append("</SecondsRunning>");
        builder.append("<NetworkConnections>");
        builder.append(nbNetworkConnection);
        builder.append("</NetworkConnections>");
        builder.append("<NbThreads>");
        builder.append(nbThread);
        builder.append("</NbThreads>");
        builder.append("<InBandwidth>");
        builder.append(bandwidthIn);
        builder.append("</InBandwidth>");
        builder.append("<OutBandwidth>");
        builder.append(bandwidthOut);
        builder.append("</OutBandwidth>");
        
        // Overall status including past, future and current transfers
        builder.append("<OVERALL>");
        builder.append("<AllTransfer>");
        builder.append(nbCountStepAllTransfer);
        builder.append("</AllTransfer>");
        builder.append("<Unknown>");
        builder.append(nbCountInfoUnknown);
        builder.append("</Unknown>");
        builder.append("<NotUpdated>");
        builder.append(nbCountInfoNotUpdated);
        builder.append("</NotUpdated>");
        builder.append("<Interrupted>");
        builder.append(nbCountInfoInterrupted);
        builder.append("</Interrupted>");
        builder.append("<ToSubmit>");
        builder.append(nbCountInfoToSubmit);
        builder.append("</ToSubmit>");
        builder.append("<Error>");
        builder.append(nbCountInfoError);
        builder.append("</Error>");
        builder.append("<Running>");
        builder.append(nbCountInfoRunning);
        builder.append("</Running>");
        builder.append("<Done>");
        builder.append(nbCountInfoDone);
        builder.append("</Done>");
        builder.append("</OVERALL>");

        // Current situation of all transfers, running or not
        builder.append("<STEPS>");
        builder.append("<Notask>");
        builder.append(nbCountStepNotask);
        builder.append("</Notask>");
        builder.append("<Pretask>");
        builder.append(nbCountStepPretask);
        builder.append("</Pretask>");
        builder.append("<Transfer>");
        builder.append(nbCountStepTransfer);
        builder.append("</Transfer>");
        builder.append("<Posttask>");
        builder.append(nbCountStepPosttask);
        builder.append("</Posttask>");
        builder.append("<AllDone>");
        builder.append(nbCountStepAllDone);
        builder.append("</AllDone>");
        builder.append("<Error>");
        builder.append(nbCountStepError);
        builder.append("</Error>");
        builder.append("</STEPS>");

        // On Running Transfers only
        builder.append("<RUNNINGSTEPS>");
        builder.append("<AllRunning>");
        builder.append(nbCountAllRunningStep);
        builder.append("</AllRunning>");
        if (detail) {
            builder.append("<Running>");
            builder.append(nbCountRunningStep);
            builder.append("</Running>");
            builder.append("<InitOk>");
            builder.append(nbCountInitOkStep);
            builder.append("</InitOk>");
            builder.append("<PreProcessingOk>");
            builder.append(nbCountPreProcessingOkStep);
            builder.append("</PreProcessingOk>");
            builder.append("<TransferOk>");
            builder.append(nbCountTransferOkStep);
            builder.append("</TransferOk>");
            builder.append("<PostProcessingOk>");
            builder.append(nbCountPostProcessingOkStep);
            builder.append("</PostProcessingOk>");
            builder.append("<CompleteOk>");
            builder.append(nbCountCompleteOkStep);
            builder.append("</CompleteOk>");
        }
        builder.append("</RUNNINGSTEPS>");
        
        if (detail) {
            // Error Status on all transfers
            builder.append("<ERRORTYPES>");
            builder.append("<ConnectionImpossible>");
            builder.append(nbCountStatusConnectionImpossible);
            builder.append("</ConnectionImpossible>");
            builder.append("<ServerOverloaded>");
            builder.append(nbCountStatusServerOverloaded);
            builder.append("</ServerOverloaded>");
            builder.append("<BadAuthent>");
            builder.append(nbCountStatusBadAuthent);
            builder.append("</BadAuthent>");
            builder.append("<ExternalOp>");
            builder.append(nbCountStatusExternalOp);
            builder.append("</ExternalOp>");
            builder.append("<TransferError>");
            builder.append(nbCountStatusTransferError);
            builder.append("</TransferError>");
            builder.append("<MD5Error>");
            builder.append(nbCountStatusMD5Error);
            builder.append("</MD5Error>");
            builder.append("<Disconnection>");
            builder.append(nbCountStatusDisconnection);
            builder.append("</Disconnection>");
            builder.append("<FinalOp>");
            builder.append(nbCountStatusFinalOp);
            builder.append("</FinalOp>");
            builder.append("<Unimplemented>");
            builder.append(nbCountStatusUnimplemented);
            builder.append("</Unimplemented>");
            builder.append("<Internal>");
            builder.append(nbCountStatusInternal);
            builder.append("</Internal>");
            builder.append("<Warning>");
            builder.append(nbCountStatusWarning);
            builder.append("</Warning>");
            builder.append("<QueryAlreadyFinished>");
            builder.append(nbCountStatusQueryAlreadyFinished);
            builder.append("</QueryAlreadyFinished>");
            builder.append("<QueryStillRunning>");
            builder.append(nbCountStatusQueryStillRunning);
            builder.append("</QueryStillRunning>");
            builder.append("<KnownHost>");
            builder.append(nbCountStatusNotKnownHost);
            builder.append("</KnownHost>");
            builder.append("<RemotelyUnknown>");
            builder.append(nbCountStatusQueryRemotelyUnknown);
            builder.append("</RemotelyUnknown>");
            builder.append("<CommandNotFound>");
            builder.append(nbCountStatusCommandNotFound);
            builder.append("</CommandNotFound>");
            builder.append("<PassThroughMode>");
            builder.append(nbCountStatusPassThroughMode);
            builder.append("</PassThroughMode>");
            builder.append("<RemoteShutdown>");
            builder.append(nbCountStatusRemoteShutdown);
            builder.append("</RemoteShutdown>");
            builder.append("<Shutdown>");
            builder.append(nbCountStatusShutdown);
            builder.append("</Shutdown>");
            builder.append("<RemoteError>");
            builder.append(nbCountStatusRemoteError);
            builder.append("</RemoteError>");
            builder.append("<Stopped>");
            builder.append(nbCountStatusStopped);
            builder.append("</Stopped>");
            builder.append("<Canceled>");
            builder.append(nbCountStatusCanceled);
            builder.append("</Canceled>");
            builder.append("<FileNotFound>");
            builder.append(nbCountStatusFileNotFound);
            builder.append("</FileNotFound>");
            builder.append("<Unknown>");
            builder.append(nbCountStatusUnknown);
            builder.append("</Unknown>");
            builder.append("</ERRORTYPES>");
        }
        builder.append("</STATUS>");
        return builder.toString();
    }
}
