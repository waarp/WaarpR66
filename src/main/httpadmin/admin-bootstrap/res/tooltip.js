var tooltipNames = [];
var tooltipRefs = [];
function createTooltip(name, ref) {
	$('#'+name).attr("data-toggle", "tooltip");
	tooltipNames.push('#'+name);
	tooltipRefs.push(ref);
};
var tooltipComplexNames = [];
var tooltipComplexRefs = [];
var tooltipComplexTexts = [];
function createTooltipText(name, ref, text) {
	$('#'+name).attr("data-toggle", "tooltiptext");
	tooltipComplexNames.push('#'+name);
	tooltipComplexRefs.push(ref);
	tooltipComplexTexts.push(text);
};
var ErrorCode;
var doFinal = function() {
	$('[xdata-i18n]').each(function() { $(this).attr("data-i18n", $(this).attr("xdata-18n")); });
	$("*").i18n();
	for (var i = 0; i < tooltipNames.length; i++) {
		$(tooltipNames[i]).attr("title", i18n.t(tooltipRefs[i]));
	}
	for (var i = 0; i < tooltipComplexNames.length; i++) {
		var refs = tooltipComplexRefs[i];
		var texts = tooltipComplexTexts[i];
		var text = "<small>";
		for (var j = 0; j < refs.length; j++) {
			if (refs[j]) { text += i18n.t(refs[j]);}
			if (texts[j]) { text += texts[j];}
		}
		text += "</small>";
		$(tooltipComplexNames[i]).attr("title", text);
	}
	$('[data-toggle="tooltiptext"]').tooltip({placement : 'bottom', container: 'body', html: true, trigger: 'hover focus', delay: {show: 0, hide: 1000}});
	$('[data-toggle="tooltip"]').tooltip({placement : 'top', container: 'body'});
	ErrorCode = { "i": i18n.t("ErrorCode.0"), "B": i18n.t("ErrorCode.1"), "X": i18n.t("ErrorCode.2"), 
		"P": i18n.t("ErrorCode.3"), "O": i18n.t("ErrorCode.4"), "C": i18n.t("ErrorCode.5"),
		"l": i18n.t("ErrorCode.6"), "A": i18n.t("ErrorCode.7"), "E": i18n.t("ErrorCode.8"),
		"T": i18n.t("ErrorCode.9"), "M": i18n.t("ErrorCode.10"), "D": i18n.t("ErrorCode.11"),
		"F": i18n.t("ErrorCode.13"), "U": i18n.t("ErrorCode.14"), "S": i18n.t("ErrorCode.15"),
		"R": i18n.t("ErrorCode.16"), "I": i18n.t("ErrorCode.17"), "H": i18n.t("ErrorCode.18"),
		"K": i18n.t("ErrorCode.19"), "W": i18n.t("ErrorCode.20"), "-": i18n.t("ErrorCode.21"),
		"Q": i18n.t("ErrorCode.22"), "s": i18n.t("ErrorCode.23"), "N": i18n.t("ErrorCode.24"),
		"L": i18n.t("ErrorCode.25"), "u": i18n.t("ErrorCode.26"), "f": i18n.t("ErrorCode.27"),
		"c": i18n.t("ErrorCode.28"), "p": i18n.t("ErrorCode.29"), "z": i18n.t("ErrorCode.30"),
		"n": i18n.t("ErrorCode.31"), "a": i18n.t("ErrorCode.32"), "d": i18n.t("ErrorCode.33") };
	if (typeof prepareTable != 'undefined') prepareTable();
}
//implement JSON.stringify serialization
JSON.stringify = JSON.stringify || function (obj) {
	var t = typeof (obj);
	if (t != "object" || obj === null) {
		// simple data type
		if (t == "string") obj = '"'+obj+'"';
		return String(obj);
	} else {
		// recurse array or object
		var v, json = [], arr = (obj && obj.constructor == Array);
		for (var n in obj) {
			v = obj[n]; t = typeof(v);
			if (t == "string") v = '"'+v+'"';
			else if (t == "object" && v !== null) v = JSON.stringify(v);
			json.push((arr ? "" : '"' + n + '":') + String(v));
		}
		return (arr ? "[" : "{") + String(json) + (arr ? "]" : "}");
	}
};
var renderTimestamp = function(data, type, full, meta) {return new Date(data).toISOString().replace('T', ' ');};
var TASKSTEP = ['NOTASK', 'PRETASK', 'TRANSFERTASK', 'POSTTASK', 'ALLDONETASK', 'ERRORTASK'];
var TASKSTEPClass = ["warning", "info", "active", "info", "success", "danger"];
var TASKSTEPColor = ["Orange", "Yellow", "LightGreen", "Turquoise", "Cyan", "#F78181"];
var createdCellStepColor = function (td, cellData, rowData, row, col) {$(td).css("background-color", TASKSTEPColor[cellData]);};
var TRANSFERMODE = ['UNKNOWNMODE', 'SENDMODE', 'RECVMODE', 'SENDMD5MODE', 'RECVMD5MODE', 'SENDTHROUGHMODE', 'RECVTHROUGHMODE', 'SENDMD5THROUGHMODE', 'RECVMD5THROUGHMODE'];
var TRANSFERMODE_Option = ['unknown', 'send', 'recv', 'sendmd5', 'recvmd5', 'sendth', 'recvth', 'sendthmd5', 'recvthmd5'];
var UpdatedInfo = ['UNKNOWN','NOTUPDATED','INTERRUPTED','TOSUBMIT','INERROR','RUNNING','DONE'];
var UpdatedInfoClass = ["info", "info", "warning", "active", "danger", "active", "success"];
var UpdatedInfoColor = ["Turquoise", "Yellow", "Orange", "Turquoise", "#F78181", "LightGreen", "Cyan"];
var createdCellInfoColor = function (td, cellData, rowData, row, col) {$(td).css("background-color", UpdatedInfoColor[cellData]);};
