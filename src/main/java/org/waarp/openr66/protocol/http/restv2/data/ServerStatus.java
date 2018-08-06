/*
 * This file is part of Waarp Project (named also Waarp or GG).
 *
 * Copyright 2009, Waarp SAS, and individual contributors by the @author
 * tags. See the COPYRIGHT.txt in the distribution for a full listing of
 * individual contributors.
 *
 * All Waarp Project is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * Waarp is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Waarp . If not, see <http://www.gnu.org/licenses/>.
 */

package org.waarp.openr66.protocol.http.restv2.data;

import org.waarp.openr66.protocol.configuration.Messages;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.handler.ServerHandler;
import org.waarp.openr66.protocol.http.restv2.testdatabases.RulesDatabase;
import org.waarp.openr66.protocol.http.restv2.testdatabases.TransfersDatabase;

import java.util.GregorianCalendar;

/** A POJO representing the general status of the R66 server. */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ServerStatus {
    public class Overall {
        public int allTransfer;

        public int unknown;

        public int notUpdated;

        public int interrupted;

        public int toSubmit;

        public int error;

        public int running;

        public int done;

        public int inRunning;

        public int outRunning;

        public GregorianCalendar lastInRunning;

        public GregorianCalendar lastOutRunning;

        public int inAll;

        public int outAll;

        public int inError;

        public int outError;

        public Overall() {
            for (Transfer transfer : TransfersDatabase.transfersDb) {
                if (ServerStatus.this.fromDate.before(transfer.startTrans)) {
                    this.allTransfer++;
                    try {
                        switch (transfer.status) {
                            case notUpdated:
                                this.notUpdated++;
                                break;
                            case interrupted:
                                this.interrupted++;
                                break;
                            case toSubmit:
                                this.toSubmit++;
                                break;
                            case running:
                                this.running++;
                                break;
                            case done:
                                this.done++;
                                break;
                            case inError:
                                this.error++;
                                break;
                            case unknown:
                                this.unknown++;
                                break;
                            default:
                                this.unknown++;
                                break;
                        }
                    } catch (ExceptionInInitializerError e) {
                        System.err.println(e.getException().getLocalizedMessage());
                        System.err.println(Messages.getSlocale() + " $");
                        System.exit(69);
                    }
                    Rule rule = RulesDatabase.select(transfer.ruleID);
                    if(rule != null) {
                        Rule.ModeTrans modeTrans = rule.modeTrans;
                        switch (modeTrans) {
                            case send:
                            case send_md5:
                                this.outAll++;
                                if (this.lastOutRunning == null || this.lastOutRunning.before(transfer.startTrans)) {
                                    this.lastOutRunning = (GregorianCalendar) transfer.startTrans;
                                }
                                if (transfer.status == Transfer.Status.running) {
                                    this.outRunning++;
                                } else if (transfer.status == Transfer.Status.inError) {
                                    this.outError++;
                                }
                                break;
                            case receive:
                            case receive_md5:
                                this.inAll++;
                                if (this.lastInRunning == null || this.lastInRunning.before(transfer.startTrans)) {
                                    this.lastInRunning = (GregorianCalendar) transfer.startTrans;
                                }
                                if (transfer.status == Transfer.Status.running) {
                                    this.inRunning++;
                                } else if (transfer.status == Transfer.Status.inError) {
                                    this.outError++;
                                }
                        }
                    }
                }
            }
        }

        public String getLastInRunning() {
            return RestUtils.fromCalendar(this.lastInRunning);
        }

        public String getLastOutRunning() {
            return RestUtils.fromCalendar(this.lastOutRunning);
        }
    }

    public class Steps {
        public int noTask;

        public int preTask;

        public int transfer;

        public int postTask;

        public int allDone;

        public int error;

        public Steps() {
            for (Transfer transfer : TransfersDatabase.transfersDb) {
                switch (transfer.globalStep) {
                    case noTask:
                        this.noTask++;
                        break;
                    case preTask:
                        this.preTask++;
                        break;
                    case transfer:
                        this.transfer++;
                        break;
                    case postTask:
                        this.postTask++;
                        break;
                    case allDone:
                        this.allDone++;
                        break;
                    case error:
                        this.error++;
                        break;
                }
            }
        }
    }

    public class RunningSteps {
        public int allRunning;

        public int running;

        public int initOk;

        public int preProcessingOk;

        public int transferOk;

        public int postProcessingOk;

        public int completeOk;

        public RunningSteps() {
            for (Transfer transfer : TransfersDatabase.transfersDb) {
                switch (transfer.step) {
                    case running:
                        this.running++;
                        break;
                    case initOK:
                        this.initOk++;
                        break;
                    case preProcessingOK:
                        this.preProcessingOk++;
                        break;
                    case transferOK:
                        this.transferOk++;
                        break;
                    case postProcessingOK:
                        this.postProcessingOk++;
                        break;
                    case completeOK:
                        this.completeOk++;
                        break;
                }
            }
            this.allRunning = this.running + this.initOk + this.preProcessingOk + this.transferOk +
                    this.postProcessingOk;
        }
    }

    public class ErrorTypes {
        public int connectionImpossible;

        public int serverOverloaded;

        public int badAuthent;

        public int externalOp;

        public int transferError;

        public int md5Error;

        public int disconnection;

        public int finalOp;

        public int unimplemented;

        public int internal;

        public int warning;

        public int queryAlreadyFinished;

        public int queryStillRunning;

        public int unknownHost;

        public int remotelyUnknown;

        public int commandNotFound;

        public int passThroughMode;

        public int remoteShutdown;

        public int shutdown;

        public int remoteError;

        public int stopped;

        public int canceled;

        public int fileNotFound;

        public int unknown;

        public ErrorTypes() {
            for (Transfer transfer : TransfersDatabase.transfersDb) {
                if ("ConnectionImpossible".equals(transfer.stepStatus)) {
                    this.connectionImpossible++;
                } else if ("ServerOverloaded".equals(transfer.stepStatus)) {
                    this.serverOverloaded++;
                } else if ("BadAuthent".equals(transfer.stepStatus)) {
                    this.badAuthent++;
                } else if ("ExternalOp".equals(transfer.stepStatus)) {
                    this.externalOp++;
                } else if ("TransferError".equals(transfer.stepStatus)) {
                    this.transferError++;
                } else if ("MD5Error".equals(transfer.stepStatus)) {
                    this.md5Error++;
                } else if ("Disconnection".equals(transfer.stepStatus)) {
                    this.disconnection++;
                } else if ("RemoteShutdown".equals(transfer.stepStatus)) {
                    this.remoteShutdown++;
                } else if ("FinalOp".equals(transfer.stepStatus)) {
                    this.finalOp++;
                } else if ("Unimplemented".equals(transfer.stepStatus)) {
                    this.unimplemented++;
                } else if ("Shutdown".equals(transfer.stepStatus)) {
                    this.shutdown++;
                } else if ("RemoteError".equals(transfer.stepStatus)) {
                    this.remoteError++;
                } else if ("Internal".equals(transfer.stepStatus)) {
                    this.internal++;
                } else if ("StoppedTransfer".equals(transfer.stepStatus)) {
                    this.stopped++;
                } else if ("CanceledTransfer".equals(transfer.stepStatus)) {
                    this.canceled++;
                } else if ("Warning".equals(transfer.stepStatus)) {
                    this.warning++;
                } else if ("Unknown".equals(transfer.stepStatus)) {
                    this.unknown++;
                } else if ("QueryAlreadyFinished".equals(transfer.stepStatus)) {
                    this.queryAlreadyFinished++;
                } else if ("QueryStillRunning".equals(transfer.stepStatus)) {
                    this.queryStillRunning++;
                } else if ("NotKnownHost".equals(transfer.stepStatus)) {
                    this.unknownHost++;
                } else if ("QueryRemotelyUnknown".equals(transfer.stepStatus)) {
                    this.remotelyUnknown++;
                } else if ("FileNotFound".equals(transfer.stepStatus)) {
                    this.fileNotFound++;
                } else if ("CommandNotFound".equals(transfer.stepStatus)) {
                    this.commandNotFound++;
                } else if ("PassThroughMode".equals(transfer.stepStatus)) {
                    this.passThroughMode++;
                }
                /*
                ErrorCode errorCode = ErrorCode.getFromCode(transfer.stepStatus);
                switch (errorCode) {
                    case ConnectionImpossible:
                        this.connectionImpossible++;
                        break;
                    case ServerOverloaded:
                        this.serverOverloaded++;
                        break;
                    case BadAuthent:
                        this.badAuthent++;
                        break;
                    case ExternalOp:
                        this.externalOp++;
                        break;
                    case TransferError:
                        this.transferError++;
                        break;
                    case MD5Error:
                        this.md5Error++;
                        break;
                    case Disconnection:
                        this.disconnection++;
                        break;
                    case RemoteShutdown:
                        this.remoteShutdown++;
                        break;
                    case FinalOp:
                        this.finalOp++;
                        break;
                    case Unimplemented:
                        this.unimplemented++;
                        break;
                    case Shutdown:
                        this.shutdown++;
                        break;
                    case RemoteError:
                        this.remoteError++;
                        break;
                    case Internal:
                        this.internal++;
                        break;
                    case StoppedTransfer:
                        this.stopped++;
                        break;
                    case CanceledTransfer:
                        this.canceled++;
                        break;
                    case Warning:
                        this.warning++;
                        break;
                    case Unknown:
                        this.unknown++;
                        break;
                    case QueryAlreadyFinished:
                        this.queryAlreadyFinished++;
                        break;
                    case QueryStillRunning:
                        this.queryStillRunning++;
                        break;
                    case NotKnownHost:
                        this.unknownHost++;
                        break;
                    case QueryRemotelyUnknown:
                        this.remotelyUnknown++;
                        break;
                    case FileNotFound:
                        this.fileNotFound++;
                        break;
                    case CommandNotFound:
                        this.commandNotFound++;
                        break;
                    case PassThroughMode:
                        this.passThroughMode++;
                        break;
                }
                */
            }
        }
    }

    public String hostID;

    public boolean running;

    public boolean useSSL;

    public boolean useNoSSL;

    public GregorianCalendar date;

    public GregorianCalendar fromDate;

    public int secondsRunning;

    public int networkConnections;

    public int nbThreads;

    public long downBandwidth;

    public long upBandwidth;

    public Overall overall;

    public Steps steps;

    public RunningSteps runningSteps;

    public ErrorTypes errorTypes;

    public ServerStatus() {
        this.hostID = RestUtils.HOST_ID;
        this.date = new GregorianCalendar();
        this.fromDate = new GregorianCalendar();
        this.fromDate.setTimeInMillis(this.fromDate.getTimeInMillis() - ServerHandler.period);
        this.secondsRunning = (int) (((new GregorianCalendar()).getTimeInMillis() -
                ServerHandler.startDate.getTimeInMillis()) / 1000);
        this.overall = new Overall();
        this.runningSteps = new RunningSteps();
        this.steps = new Steps();
        this.errorTypes = new ErrorTypes();
    }

    public String getDate() {
        return RestUtils.fromCalendar(this.date);
    }

    public String getFromDate() {
        return RestUtils.fromCalendar(this.fromDate);
    }
}
