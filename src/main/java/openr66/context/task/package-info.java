/**
 * Classes implementing Tasks for pre, post or error operations
 *
 *
 * Several kind of tasks exist in OpenR66:<br>
 * TEST, MOVE, MOVERENAME, COPY, COPYRENAME, EXEC, EXECMOVE, LINKRENAME<br><br>
 * <br><br>
 *
 * Several tasks are possible to run before a transfer starts (pre action),
 * after a transfer is finished correctly (post action) or after an error occurs
 * (either in pre or post action or during transfer: error action).
 *
 * Those actions are defined in one rule. Each rule contains 2 parts:
 * <ul><li>Sender actions: A host is a Sender if it is the requester on a SEND rule or
 * if it is the requested on a RECV rule.</li>
 * <li>Receiver actions: A host is a Sender if it is the requester on a RECV rule or
 * if it is the requested on a SEND rule.</li>
 * </ul>
 *
 * Each action could be on pre, post or error step, each step can have several actions.
 *
 * It is defined with a unified form of XML:<br><xmp>
 *    <tasks>
 *     <task>
 *      <type>NAME</type>
 *      <path>path</path>
 *      <delay>x</delay>
 *     </task>
 *     <task>
 *      <type>NAME</type>
 *      <path>path</path>
 *      <delay>x</delay>
 *     </task>
 *    </tasks></xmp><br>
 * Where
 * <ul><li>Type is the type of task to execute (see below the supported types)</li>
 * <li>Path is a fixed argument for the task to execute. On this argument, string
 * replacements are done when the following patterns are found:<ul>
 * <li>#TRUEFULLPATH# : Current full path of current FILENAME</li>
 * <li>#TRUEFILENAME# : Current FILENAME (basename) (change in retrieval part)</li>
 * <li>#ORIGINALFILENAME# : Original FILENAME (basename) (before changing in retrieval part)</li>
 * <li>#FILESIZE# : File size if it exists</li>
 * <li>#RULE# : Rule used during transfer</li>
 * <li>#DATE# : Current Date in yyyyMMdd format</li>
 * <li>#HOUR# : Current Hour in HHmmss format</li>
 * <li>#REMOTEHOST# : Remote host Id (if not the initiator of the call)</li>
 * <li>#LOCALHOST# : Local host Id</li>
 * <li>#TRANSFERID# : Transfer Id</li>
 * <li>#REQUESTERHOST# : Requester host Id</li>
 * <li>#REQUESTEDHOST# : Requested host Id</li>
 * <li>#FULLTRANSFERID# : Full Transfer Id as TRANSFERID_REQUESTERHOST_REQUESTEDHOST</li>
 * <li>#RANKTRANSFER# : Current or final RANK of block</li>
 * <li>#BLOCKSIZE# : Block size used</li>
 * </ul></li>
 * <li>Delay is generally the delay (if any) for execution before the execution
 * becomes out of time.</li>
 * <li>Additionnaly, a task will use also the argument from the transfer itself
 * (Transfer Information).</li>
 * </ul><br>
 *
 * The different kinds of TASK are:<br>
 * <ul>
 * <li>TEST</li> This task logs some information only if delay is > 0.
 * <li>MOVE</li> Move the file to the path designed by Path argument without renaming the filename (same basename). Delay and Transfer Information are ignored.
 * The file is marked as moved.
 * <li>MOVERENAME</li> Move the file to the path designed by Path and Transfer Information arguments. Delay is ignored.
 * After Path is transformed according to above dynamic replacements, it is then used as a String Format
 * where Transfer Information is used as input (String.format(Path,Info)). The file is marked as moved.
 * <li>COPY</li> Copy the file to the path designed by Path argument without renaming the filename (same basename). Delay and Transfer Information are ignored.
 * The file is not marked as moved.
 * <li>COPYRENAME</li> Copy the file to the path designed by Path and Transfer Information arguments. Delay is ignored.
 * After Path is transformed according to above dynamic replacements, it is then used as a String Format
 * where Transfer Information is used as input (String.format(Path,Info)). The file is not marked as moved.
 * <li>EXEC</li> Execute an external command given by Path and Transfer Information arguments. The Delay is the maximum amount of time in milliseconds before the task
 * should be considered as over time and so in error.<br>
 * The command path is obtained from Path transformed according to above dynamic replacements, and after a String Format
 * where Transfer Information is used as input (String.format(Path,Info)).
 * <li>EXECMOVE</li> Execute an external command given by Path and Transfer Information arguments. The Delay is the maximum amount of time in milliseconds before the task
 * should be considered as over time and so in error.<br>
 * The command path is obtained from Path transformed according to above dynamic replacements, and after a String Format
 * where Transfer Information is used as input (String.format(Path,Info)).<br>
 * The last line returned by the external command is interpreted as the new full file path. The external command
 * is responsible to really move the previous file to the new one. The file is marked as moved.
 * <li>LINKRENAME</li> Create a link of the current file and make the file pointing to it.
 * The link first tries to be a hard link, then a soft link, and if it is really not
 * possible (not supported by the filesystem), it does a copy and rename task.<br>
 * Delay is ignored.
 * After Path is transformed according to above dynamic replacements, it is then used as a String Format
 * where Transfer Information is used as input (String.format(Path,Info)). The file is not marked as moved.
 * </ul><br><br>
 *
 *
 * @apiviz.landmark
 */
package openr66.context.task;

