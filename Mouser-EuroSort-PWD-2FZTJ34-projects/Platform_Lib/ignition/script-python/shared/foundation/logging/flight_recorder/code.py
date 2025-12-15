# shared/foundation/logging/flight_recorder.py
# Newline-delimited JSON Flight Recorder (Jython-safe)

from shared.foundation.time import clock

try:
	import java.io.File as JFile
	import java.io.FileOutputStream as FileOutputStream
	import java.io.OutputStreamWriter as OutputStreamWriter
	import java.io.BufferedWriter as BufferedWriter
	import java.lang.System as JSystem
except:
	JFile = None
	FileOutputStream = None
	OutputStreamWriter = None
	BufferedWriter = None
	JSystem = None


LEVELS = {
	"DEBUG": 10,
	"INFO": 20,
	"WARN": 30,
	"WARNING": 30,
	"ERROR": 40,
	"CRITICAL": 50,
}


def _level_value(level):
	try:
		return int(LEVELS.get(str(level or "").upper(), 20))
	except:
		return 20


def _safe_json(obj):
	"""
	Jython-safe JSON encode.
	Prefers Ignition system.util.jsonEncode if available.
	Falls back to a minimal encoder.

	IMPORTANT:
	- system.util.jsonEncode can throw if obj contains non-serializable Java/Python objects
	- We attempt a sanitize pass first
	"""
	try:
		import system
		try:
			return system.util.jsonEncode(_sanitize(obj))
		except:
			# Try raw as-is (sometimes sanitize is the thing that broke)
			return system.util.jsonEncode(obj)
	except:
		pass

	# Minimal fallback (handles dict/list/str/num/bool/None)
	try:
		return _encode_min(obj)
	except:
		try:
			return "\"%s\"" % str(obj).replace("\\", "\\\\").replace("\"", "\\\"")
		except:
			return "\"<unencodable>\""


def _sanitize(x, depth=0, max_depth=8):
	"""
	Best-effort conversion of weird objects into JSON-safe primitives.
	Prevents jsonEncode from throwing on unexpected Java objects.
	"""
	if depth >= max_depth:
		try:
			return str(x)
		except:
			return "<max_depth>"

	if x is None or x is True or x is False:
		return x

	# numbers
	try:
		if isinstance(x, (int, long, float)):
			return x
	except:
		pass

	# strings
	try:
		if isinstance(x, basestring):
			return x
	except:
		pass

	# dict
	if isinstance(x, dict):
		out = {}
		for k, v in x.items():
			try:
				out[str(k)] = _sanitize(v, depth=depth + 1, max_depth=max_depth)
			except:
				out[str(k)] = "<unencodable>"
		return out

	# list/tuple
	if isinstance(x, (list, tuple)):
		out = []
		for v in x:
			try:
				out.append(_sanitize(v, depth=depth + 1, max_depth=max_depth))
			except:
				out.append("<unencodable>")
		return out

	# fallback
	try:
		return str(x)
	except:
		return "<unencodable>"


def _encode_min(x):
	if x is None:
		return "null"
	if x is True:
		return "true"
	if x is False:
		return "false"

	# numbers
	try:
		if isinstance(x, (int, long, float)):
			return str(x)
	except:
		pass

	# strings
	try:
		if isinstance(x, basestring):
			return "\"%s\"" % str(x).replace("\\", "\\\\").replace("\"", "\\\"")
	except:
		pass

	# dict
	if isinstance(x, dict):
		parts = []
		for k, v in x.items():
			parts.append("%s:%s" % (_encode_min(str(k)), _encode_min(v)))
		return "{%s}" % ",".join(parts)

	# list/tuple
	if isinstance(x, (list, tuple)):
		return "[%s]" % ",".join([_encode_min(v) for v in x])

	# fallback to string
	return "\"%s\"" % str(x).replace("\\", "\\\\").replace("\"", "\\\"")


def _default_base_dir():
	"""
	Best-effort default directory for gateway-safe logging.
	Override via config if you want a specific path.
	"""
	try:
		if JSystem is not None:
			home = str(JSystem.getProperty("ignition.home"))
			if home and home != "null":
				return home + "/data/logs/es_platform"
	except:
		pass

	try:
		if JSystem is not None:
			udir = str(JSystem.getProperty("user.dir"))
			if udir and udir != "null":
				return udir + "/logs/es_platform"
	except:
		pass

	return "./logs/es_platform"


class FlightRecorder(object):
	"""
	Flight Recorder:
	- Writes JSONL lines to a file for troubleshooting
	- When enabled=False, it records only WARN/ERROR by default
	- When enabled=True, it records >= min_level (default INFO)

	period_provider() -> string key (ex: YYYYMMDD-DAY). If None, uses YYYYMMDD.
	"""

	def __init__(self,
			systemCode,
			base_dir=None,
			enabled=False,
			min_level="INFO",
			min_level_when_disabled="WARN",
			period_provider=None,
			max_bytes=25 * 1024 * 1024,
			flush_each_write=True,
			site_tz_id="UTC",
			filename_prefix="ES_Platform"):
		self.systemCode = str(systemCode)
		self.base_dir = str(base_dir or _default_base_dir())
		self.enabled = bool(enabled)

		self.min_level = str(min_level or "INFO").upper()
		self.min_level_when_disabled = str(min_level_when_disabled or "WARN").upper()

		self.period_provider = period_provider

		self.max_bytes = int(max_bytes or 0)
		self.flush_each_write = bool(flush_each_write)
		self.site_tz_id = str(site_tz_id or "UTC")

		self.filename_prefix = str(filename_prefix or "ES_Platform")

		self._current_key = None
		self._file = None
		self._writer = None
		self._bytes = 0
		self._roll_index = 0

	def set_enabled(self, enabled):
		self.enabled = bool(enabled)
		return {"ok": True, "enabled": self.enabled}

	def set_min_level(self, level):
		self.min_level = str(level or "INFO").upper()
		return {"ok": True, "min_level": self.min_level}

	def status(self):
		return {
			"systemCode": self.systemCode,
			"base_dir": self.base_dir,
			"enabled": self.enabled,
			"min_level": self.min_level,
			"min_level_when_disabled": self.min_level_when_disabled,
			"current_key": self._current_key,
			"current_bytes": self._bytes,
			"max_bytes": self.max_bytes,
			"roll_index": self._roll_index,
		}

	# ----------------------------
	# Public API
	# ----------------------------

	def record(self, level, message, payload=None, eventType=None, entityType=None, entityId=None, userId=None, eventId=None, corrId=None):
		"""
		Generic record line (logger-style).
		"""
		if not self._should_record(level):
			return {"ok": True, "skipped": True}

		ts = clock.pack_timestamps(tz_id=self.site_tz_id)
		doc = {
			"kind": "LOG",
			"systemCode": self.systemCode,
			"level": str(level or "INFO").upper(),
			"message": str(message),
			"payload": payload,

			"eventType": eventType,
			"entityType": entityType,
			"entityId": entityId,

			"userId": userId,
			"eventId": eventId,
			"corrId": corrId or eventId,

			"tsEpoch": ts.get("tsEpoch"),
			"tsLocal": ts.get("tsLocal"),
			"tsUtc": ts.get("tsUtc"),
			"tzId": ts.get("tzId"),
		}
		return self._write_doc(doc)

	def record_event(self, event_doc, level="INFO"):
		"""
		Special helper to mirror platform EventEmitter docs.
		"""
		if not self._should_record(level):
			return {"ok": True, "skipped": True}

		doc = dict(event_doc or {})
		doc["kind"] = doc.get("kind") or "EVENT"
		doc["systemCode"] = doc.get("systemCode") or self.systemCode
		doc["level"] = str(level or "INFO").upper()
		return self._write_doc(doc)

	def close(self):
		self._close_writer()
		return {"ok": True, "closed": True}

	# ----------------------------
	# Internals
	# ----------------------------

	def _should_record(self, level):
		lv = _level_value(level)
		if self.enabled:
			return lv >= _level_value(self.min_level)
		return lv >= _level_value(self.min_level_when_disabled)

	def _period_key(self):
		try:
			if self.period_provider:
				k = self.period_provider()
				if k:
					return str(k)
		except:
			pass

		ts = clock.pack_timestamps(tz_id=self.site_tz_id)
		local = str(ts.get("tsLocal") or "")
		try:
			return local.split(" ")[0]
		except:
			return local[:8] if len(local) >= 8 else "UNKNOWN"

	def _ensure_writer(self):
		key = self._period_key()
		if self._writer is not None and self._current_key == key:
			return

		self._close_writer()
		self._current_key = key
		self._roll_index = 0
		self._open_new_file()

	def _open_new_file(self):
		if JFile is None:
			return

		try:
			dirf = JFile(self.base_dir)
			if not dirf.exists():
				dirf.mkdirs()
		except:
			pass

		filename = self._build_filename()
		try:
			f = JFile(self.base_dir, filename)
			fos = FileOutputStream(f, True)  # append
			osw = OutputStreamWriter(fos, "UTF-8")
			bw = BufferedWriter(osw)

			self._file = f
			self._writer = bw

			try:
				self._bytes = int(f.length())
			except:
				self._bytes = 0
		except:
			self._file = None
			self._writer = None
			self._bytes = 0

	def _build_filename(self):
		# <prefix>-<system>-<period>.jsonl  (with optional roll index)
		key = str(self._current_key or "UNKNOWN")
		base = "%s-%s-%s" % (self.filename_prefix, self.systemCode, key)

		if self._roll_index > 0:
			return "%s-%d.jsonl" % (base, int(self._roll_index))
		return "%s.jsonl" % base

	def _close_writer(self):
		try:
			if self._writer is not None:
				try:
					self._writer.flush()
				except:
					pass
				try:
					self._writer.close()
				except:
					pass
		except:
			pass

		self._writer = None
		self._file = None
		self._bytes = 0

	def _maybe_roll(self, upcoming_len):
		if self.max_bytes <= 0:
			return False

		try:
			if int(self._bytes + upcoming_len) < int(self.max_bytes):
				return False
		except:
			return False

		self._close_writer()
		self._roll_index = int(self._roll_index) + 1
		self._open_new_file()
		return True

	def _write_doc(self, doc):
		self._ensure_writer()
		if self._writer is None:
			return {"ok": False, "error": "no_writer"}

		line = _safe_json(doc) + "\n"
		try:
			# Roll BEFORE writing if needed
			self._maybe_roll(len(line))

			if self._writer is None:
				return {"ok": False, "error": "no_writer_after_roll"}

			self._writer.write(line)

			if self.flush_each_write:
				try:
					self._writer.flush()
				except:
					pass

			try:
				self._bytes = int(self._bytes) + len(line)
			except:
				pass

			return {"ok": True}

		except Exception as e:
			return {"ok": False, "error": str(e)}