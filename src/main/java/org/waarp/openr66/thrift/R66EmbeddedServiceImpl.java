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
package org.waarp.openr66.thrift;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.thrift.TException;
import org.waarp.common.command.exception.CommandAbstractException;
import org.waarp.common.database.data.AbstractDbData;
import org.waarp.common.database.exception.WaarpDatabaseException;
import org.waarp.common.logging.WaarpInternalLogger;
import org.waarp.common.logging.WaarpInternalLoggerFactory;
import org.waarp.openr66.commander.ClientRunner;
import org.waarp.openr66.context.R66Session;
import org.waarp.openr66.context.filesystem.R66File;
import org.waarp.openr66.database.DbConstant;
import org.waarp.openr66.database.data.DbRule;
import org.waarp.openr66.database.data.DbTaskRunner;
import org.waarp.openr66.protocol.configuration.Configuration;
import org.waarp.openr66.protocol.localhandler.LocalChannelReference;
import org.waarp.openr66.protocol.localhandler.packet.ErrorPacket;
import org.waarp.openr66.protocol.localhandler.packet.RequestPacket;
import org.waarp.openr66.protocol.utils.ChannelUtils;
import org.waarp.openr66.protocol.utils.TransferUtils;
import org.waarp.thrift.r66.Action;
import org.waarp.thrift.r66.ErrorCode;
import org.waarp.thrift.r66.R66Request;
import org.waarp.thrift.r66.R66Result;
import org.waarp.thrift.r66.R66Service;
import org.waarp.thrift.r66.RequestMode;

/**
 * Embedded service attached with the Thrift service
 * @author Frederic Bregier
 *
 */
public class R66EmbeddedServiceImpl implements R66Service.Iface {
	/**
	 * Internal Logger
	 */
	private static final WaarpInternalLogger logger = WaarpInternalLoggerFactory
			.getLogger(R66EmbeddedServiceImpl.class);
	
	private DbTaskRunner initRequest(R66Request request) {
		Timestamp ttimestart = null;
		if (request.isSetStart()) {
			Date date;
			try {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
				date = dateFormat.parse(request.getStart());
				ttimestart = new Timestamp(date.getTime());
			} catch (ParseException e) {
			}
		} else if (request.isSetDelay()) {
			if (request.getDelay().charAt(0) == '+') {
				ttimestart = new Timestamp(System.currentTimeMillis() +
						Long.parseLong(request.getDelay().substring(1)));
			} else {
				ttimestart = new Timestamp(Long.parseLong(request.getDelay()));
			}
		}
		DbRule rule;
		try {
			rule = new DbRule(DbConstant.admin.session, request.getRule());
		} catch (WaarpDatabaseException e) {
			logger.warn("Cannot get Rule: " + request.getRule(), e);
			return null;
		}
		int mode = rule.mode;
		if (request.isMd5()) {
			mode = RequestPacket.getModeMD5(mode);
		}
		DbTaskRunner taskRunner = null;
		long tid = DbConstant.ILLEGALVALUE;
		if (request.isSetTid()) {
			tid = request.getTid();
		}
		if (tid != DbConstant.ILLEGALVALUE) {
			try {
				taskRunner = new DbTaskRunner(DbConstant.admin.session, tid,
						request.getDestuid());
				// requested
				taskRunner.setSenderByRequestToValidate(true);
			} catch (WaarpDatabaseException e) {
				logger.warn("Cannot get task", e);
				return null;
			}
		} else {
			RequestPacket requestPacket = new RequestPacket(request.getRule(),
					mode, request.getFile(), request.getBlocksize(), 0,
					tid, request.getInfo());
			// Not isRecv since it is the requester, so send => isRetrieve is true
			boolean isRetrieve = !RequestPacket.isRecvMode(requestPacket.getMode());
			try {
				taskRunner =
						new DbTaskRunner(DbConstant.admin.session, rule, isRetrieve, requestPacket,
								request.getDestuid(), ttimestart);
			} catch (WaarpDatabaseException e) {
				logger.warn("Cannot get task", e);
				return null;
			}
		}
		return taskRunner;
	}

	public R66Result transferRequestQuery(R66Request request) throws TException {
		DbTaskRunner runner = initRequest(request);
		if (runner != null) {
			runner.changeUpdatedInfo(AbstractDbData.UpdatedInfo.TOSUBMIT);
			boolean isSender = runner.isSender();
			if (! runner.forceSaveStatus()) {
				logger.warn("Cannot prepare task");
				return new R66Result(request.getMode(), ErrorCode.CommandNotFound, 
						"ERROR: Cannot prepare transfer");
			}
			R66Result result = new R66Result(request.getMode(), ErrorCode.InitOk, 
					"Transfer Scheduled");
			if (request.getMode() == RequestMode.SYNCTRANSFER) {
				// now need to wait but first, reload the runner
				try {
					runner.select();
					while (! runner.isFinished()) {
						try {
							Thread.sleep(1000);
							runner.select();
						} catch (InterruptedException e) {
							break;
						}
					}
					runner.setSender(isSender);
				} catch (WaarpDatabaseException e1) {
				}
				setResultFromRunner(runner, result);
				if (runner.isAllDone()) {
					result.setCode(ErrorCode.CompleteOk);
					result.setResultinfo("Transfer Done");
				} else {
					result.setCode(ErrorCode.valueOf(runner.getErrorInfo().name()));
					result.setResultinfo(runner.getErrorInfo().mesg);
				}
			} else {
				try {
					runner.select();
				} catch (WaarpDatabaseException e) {
				}
				runner.setSender(isSender);
				setResultFromRunner(runner, result);
			}
			return result;
		} else {
			logger.warn("ERROR: Transfer NOT scheduled");
			R66Result result = new R66Result(request.getMode(), ErrorCode.Internal, 
					"ERROR: Transfer NOT scheduled");
			return result;
		}
	}

	private void setResultFromRunner(DbTaskRunner runner, R66Result result) {
		result.setDestuid(runner.getRequested());
		result.setFromuid(runner.getRequester());
		result.setTid(runner.getSpecialId());
		result.setRule(runner.getRuleId());
		result.setBlocksize(runner.getBlocksize());
		result.setFile(runner.getFilename());
		result.setOriginalfilename(runner.getOriginalFilename());
		result.setIsmoved(runner.isFileMoved());
		result.setModetransfer(runner.getMode());
		result.setRetrievemode(runner.isSender());
		result.setStep(runner.getStep());
		result.setGloballaststep(runner.getGloballaststep());
		result.setRank(runner.getRank());
		result.setStart(runner.getStart().toString());
		result.setStop(runner.getStop().toString());
		result.setResultinfo(runner.getFileInformation());
	}
	
	private void setResultFromLCR(LocalChannelReference lcr, R66Result result) {
		R66Session session = lcr.getSession();
		DbTaskRunner runner = null;
		if (session != null) {
			runner = session.getRunner();
		} else {
			ClientRunner run = lcr.getClientRunner();
			if (run != null) {
				runner = run.getTaskRunner();
			}
		}
		if (runner != null) {
			setResultFromRunner(runner, result);
		}
	}
	
	private int stopOrCancelRunner(long id, String reqd, String reqr, org.waarp.openr66.context.ErrorCode code) {
		try {
			DbTaskRunner taskRunner =
					new DbTaskRunner(DbConstant.admin.session, null,
							null, id, reqr, reqd);
			return taskRunner.stopOrCancelRunner(code) ? 1 : 0;
		} catch (WaarpDatabaseException e) {
		}
		logger.warn("Cannot accomplished action on task: "+id+" "+code.name());
		return -1;
	}

	private R66Result stopOrCancel(R66Request request, LocalChannelReference lcr,
			org.waarp.openr66.context.ErrorCode r66code) {
		// stop the current transfer
		R66Result resulttest;
		if (lcr != null) {
			int rank = 0;
			if (r66code == org.waarp.openr66.context.ErrorCode.StoppedTransfer && lcr.getSession() != null) {
				DbTaskRunner taskRunner = lcr.getSession().getRunner();
				if (taskRunner != null) {
					rank = taskRunner.getRank();
				}
			}
			ErrorPacket error = new ErrorPacket(r66code.name() + " " + rank,
					r66code.getCode(), ErrorPacket.FORWARDCLOSECODE);
			try {
				// inform local instead of remote
				ChannelUtils.writeAbstractLocalPacketToLocal(lcr, error);
			} catch (Exception e) {
			}
			resulttest = new R66Result(request.getMode(), ErrorCode.CompleteOk, 
					r66code.name());
			setResultFromLCR(lcr, resulttest);
		} else {
			// Transfer is not running
			// but maybe need action on database
			int test = stopOrCancelRunner(request.getTid(), request.getDestuid(), request.getFromuid(), r66code);
			if (test > 0) {
				resulttest = new R66Result(request.getMode(), ErrorCode.CompleteOk, 
						r66code.name());
			} else if (test == 0) {
				resulttest = new R66Result(request.getMode(), ErrorCode.TransferOk, 
						r66code.name());
			} else {
				resulttest = new R66Result(request.getMode(), ErrorCode.CommandNotFound, 
						"Error: cannot accomplished task on transfer");
			}
		}
		return resulttest;
	}
	
	private R66Result restart(R66Request request, LocalChannelReference lcr) {
		// Try to validate a restarting transfer
		// validLimit on requested side
		if (Configuration.configuration.constraintLimitHandler.checkConstraints()) {
			logger.warn("Limit exceeded while asking to relaunch a task "
					+ request.toString());
			return new R66Result(request.getMode(), ErrorCode.ServerOverloaded, 
					"Limit exceeded while asking to relaunch a task");
		}
		// Try to validate a restarting transfer
		// header = ?; middle = requested+blank+requester+blank+specialId
		DbTaskRunner taskRunner = null;
		try {
			taskRunner = new DbTaskRunner(DbConstant.admin.session, null,
					null, request.getTid(), request.getFromuid(), request.getDestuid());
			org.waarp.openr66.context.R66Result resulttest = TransferUtils.restartTransfer(taskRunner, lcr);
			return new R66Result(request.getMode(), ErrorCode.valueOf(resulttest.code.name()), 
					resulttest.getMessage());
		} catch (WaarpDatabaseException e1) {
			logger.warn("Exception while trying to restart transfer", e1);
			return new R66Result(request.getMode(), ErrorCode.Internal, 
					"Exception while trying to restart transfer");
		}
	}
	
	public R66Result infoTransferQuery(R66Request request) throws TException {
		RequestMode mode = request.getMode();
		if (mode != RequestMode.INFOREQUEST) {
			// error
			logger.warn("Mode is uncompatible with infoTransferQuery");
			return new R66Result(request.getMode(), ErrorCode.Unimplemented, 
					"Mode is uncompatible with infoTransferQuery");
		}
		// now check if enough arguments are provided
		if ((!request.isSetTid()) || (!request.isSetDestuid() && !request.isSetFromuid()) ||
				(!request.isSetAction())) {
			// error
			logger.warn("Not enough arguments");
			return new R66Result(request.getMode(), ErrorCode.RemoteError, 
					"Not enough arguments");
		}
		// requested+blank+requester+blank+specialId
		LocalChannelReference lcr =
				Configuration.configuration.getLocalTransaction().
						getFromRequest(request.getDestuid()+" "+request.getFromuid()+" "+request.getTid());
		org.waarp.openr66.context.ErrorCode r66code;
		switch (request.getAction()) {
			case Detail: {
				R66Result result = new R66Result(request.getMode(), ErrorCode.CompleteOk, 
						"Existence test OK");
				result.setAction(Action.Exist);
				result.setDestuid(request.getDestuid());
				result.setFromuid(request.getFromuid());
				result.setTid(request.getTid());
				if (lcr != null) {
					setResultFromLCR(lcr, result);
				} else {
					try {
						DbTaskRunner runner = new DbTaskRunner(DbConstant.admin.session, null, 
								null, request.getTid(), request.getFromuid(), 
								request.getDestuid());
						if (runner != null) {
							setResultFromRunner(runner, result);
						}
					} catch (WaarpDatabaseException e) {
						result.setCode(ErrorCode.FileNotFound);
					}
				}
				return result;
			}
			case Restart:
				return restart(request, lcr);
			case Cancel:
				r66code = org.waarp.openr66.context.ErrorCode.CanceledTransfer;
				return stopOrCancel(request, lcr, r66code);
			case Stop:
				r66code = org.waarp.openr66.context.ErrorCode.StoppedTransfer;
				return stopOrCancel(request, lcr, r66code);
			default:
				logger.warn("Uncompatible with "+request.getAction().name());
				return new R66Result(request.getMode(), ErrorCode.Unimplemented, 
						"Uncompatible with "+request.getAction().name());
		}
	}

	public boolean isStillRunning(String fromuid, String touid, long tid) throws TException {
		// now check if enough arguments are provided
		if (fromuid == null || touid == null || tid == DbConstant.ILLEGALVALUE) {
			// error
			logger.warn("Not enough arguments");
			return false;
		}
		// header = ?; middle = requested+blank+requester+blank+specialId
		LocalChannelReference lcr =
				Configuration.configuration.getLocalTransaction().
						getFromRequest(touid+" "+fromuid+" "+tid);
		return (lcr != null);
	}

	public List<String> infoListQuery(R66Request request) throws TException {
		List<String> list = new ArrayList<String>();
		RequestMode mode = request.getMode();
		if (mode != RequestMode.INFOFILE) {
			// error
			logger.warn("Not correct mode for infoListQuery");
			list.add("Not correct mode for infoListQuery");
			return list;
		}
		// now check if enough arguments are provided
		if ((!request.isSetRule()) || (!request.isSetAction())) {
			// error
			logger.warn("Not enough arguments");
			list.add("Not enough arguments");
			return list;
		}
		R66Session session = new R66Session();
		session.getAuth().specialNoSessionAuth(false, Configuration.configuration.HOST_ID);
		DbRule rule;
		try {
			rule = new DbRule(DbConstant.admin.session, request.getRule());
		} catch (WaarpDatabaseException e) {
			logger.warn("Rule is unknown: " + request.getRule());
			list.add("Rule is unknown: " + request.getRule());
			return list;
		}
		try {
			if (RequestPacket.isRecvMode(rule.mode)) {
				session.getDir().changeDirectory(rule.workPath);
			} else {
				session.getDir().changeDirectory(rule.sendPath);
			}

			if (request.getAction() == Action.List ||
					request.getAction() == Action.Mlsx) {
				// ls or mls from current directory
				if (request.getAction() == Action.List) {
					list = session.getDir().list(request.getFile());
				} else {
					list = session.getDir().listFull(request.getFile(), false);
				}
				return list;
			} else {
				// ls pr mls from current directory and filename
				if (! request.isSetFile()) {
					logger.warn("File missing");
					list.add("File missing");
					return list;
				}
				R66File file = (R66File) session.getDir().setFile(request.getFile(), false);
				String sresult = null;
				if (request.getAction() == Action.Exist) {
					sresult = "" + file.exists();
					list.add(sresult);
				} else if (request.getAction() == Action.Detail) {
					sresult = session.getDir().fileFull(request.getFile(), false);
					String[] slist = sresult.split("\n");
					sresult = slist[1];
					list.add(sresult);
				}
				return list;
			}
		} catch (CommandAbstractException e) {
			logger.warn("Error occurs during: " + request.toString(), e);
			list.add("Error occurs during: " + request.toString());
			return list;
		}
	}

}
