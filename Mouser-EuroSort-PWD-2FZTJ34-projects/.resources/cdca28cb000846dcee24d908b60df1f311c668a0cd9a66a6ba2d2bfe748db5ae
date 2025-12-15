from shared.foundation.time import clock

try:
	import system
except:
	system = None


class ShiftResolver(object):
	"""
	Computes a cache period key.

	Modes:
	- mode="day":		YYYYMMDD
	- mode="hours":		YYYYMMDD-<SHIFTNAME> (computed from hours)
	- mode="tag":		YYYYMMDD-<TAGVALUE> or just <TAGVALUE> (configurable)

	Recommended:
	- Use mode="tag" and drive the tag from Alarm Scheduling / Hours of Operation logic.
	"""

	def __init__(self, site_tz_id="UTC", config=None, tag_reader=None, logger=None):
		self.site_tz_id = site_tz_id
		self.config = dict(config or {})
		self.tag_reader = tag_reader or IgnitionTagReader()
		self.logger = logger

	def _log(self, msg, payload=None, level="info"):
		if self.logger:
			try:
				fn = getattr(self.logger, level, None)
				if fn:
					fn(msg, payload)
					return
			except:
				pass
		try:
			print("%s %s" % (msg, payload if payload is not None else ""))
		except:
			pass

	def period_key(self):
		"""
		Returns a string key.
		Examples:
			"20251214"
			"20251214-DAY"
			"20251214-NIGHT"
			"CLOSED"
		"""
		mode = str(self.config.get("mode") or "day").lower()
		ts = clock.pack_timestamps(tz_id=self.site_tz_id)

		if mode == "tag":
			return self._period_key_from_tag(ts)

		if mode == "hours":
			return self._period_key_from_hours(ts)

		# default: day
		return _yyyymmdd(ts)

	def _period_key_from_tag(self, ts):
		path = self.config.get("tag_path")
		if not path:
			self._log("ShiftResolver(tag): missing tag_path, falling back to day key", self.config, level="warn")
			return _yyyymmdd(ts)

		val = self.tag_reader.read_one(str(path))
		val = "" if val is None else str(val).strip()

		if not val:
			# If schedule tag is blank, treat as day key
			return _yyyymmdd(ts)

		# Optional: CLOSED behavior
		if str(val).upper() in ("CLOSED", "OFF", "DISABLED"):
			return str(val).upper()

		# Choose whether to prefix with date
		prefix_date = bool(self.config.get("prefix_date", True))
		if prefix_date:
			return "%s-%s" % (_yyyymmdd(ts), val)

		return val

	def _period_key_from_hours(self, ts):
		"""
		Compute shift based on local hour/minute.

		Config examples:
		{
			"mode": "hours",
			"shifts": [
				{"name":"DAY", "start":"06:00", "end":"18:00"},
				{"name":"NIGHT", "start":"18:00", "end":"06:00"},
			]
		}
		"""
		shifts = self.config.get("shifts") or []
		if not shifts:
			self._log("ShiftResolver(hours): missing shifts list, falling back to day key", self.config, level="warn")
			return _yyyymmdd(ts)

		# Parse current local HH:MM from tsLocal (best-effort)
		local = str(ts.get("tsLocal") or "")
		hhmm = _extract_hhmm(local)
		if hhmm is None:
			return _yyyymmdd(ts)

		now_min = hhmm[0] * 60 + hhmm[1]

		for sh in shifts:
			name = str(sh.get("name") or "").strip()
			start = _parse_hhmm(sh.get("start"))
			end = _parse_hhmm(sh.get("end"))
			if not name or start is None or end is None:
				continue

			if _in_range_minutes(now_min, start, end):
				return "%s-%s" % (_yyyymmdd(ts), name)

		# If nothing matches, treat as CLOSED (optional) or day
		if bool(self.config.get("closed_when_no_match", False)):
			return "CLOSED"
		return _yyyymmdd(ts)


class IgnitionTagReader(object):
	"""
	Simple tag reader (Gateway-safe).
	"""
	def read_one(self, tag_path):
		if system is None:
			return None
		try:
			qv = system.tag.readBlocking([tag_path])[0]
			try:
				return qv.value
			except:
				# fallback
				return qv.getValue()
		except:
			return None


def _yyyymmdd(ts):
	# Extract first 8 digits from tsLocal
	local = str(ts.get("tsLocal") or "")
	digits = []
	for ch in local:
		if ch.isdigit():
			digits.append(ch)
			if len(digits) >= 8:
				break
	return "".join(digits) if len(digits) == 8 else None


def _extract_hhmm(s):
	# Extract HH:MM from a string like "20251214 17:02:11.123"
	if not s:
		return None
	try:
		parts = s.split(" ")
		if len(parts) < 2:
			return None
		time_part = parts[1]
		hh = int(time_part[0:2])
		mm = int(time_part[3:5])
		return (hh, mm)
	except:
		return None


def _parse_hhmm(v):
	# "06:00" -> 360
	if v is None:
		return None
	try:
		s = str(v).strip()
		h = int(s[0:2])
		m = int(s[3:5])
		return h * 60 + m
	except:
		return None


def _in_range_minutes(now_min, start_min, end_min):
	"""
	Handles wrap-around shifts (e.g., 18:00 -> 06:00).
	"""
	if start_min == end_min:
		return True

	if start_min < end_min:
		return (now_min >= start_min) and (now_min < end_min)

	# wrap around midnight
	return (now_min >= start_min) or (now_min < end_min)