# shared/es_platform/commands/command_queue.py

from shared.foundation.time import clock


class CommandQueue(object):
	"""
	In-memory command queue with:
	- max_size: safety
	- min_ms_between: throttle
	- dedupe_window_ms: ignore repeats of same dedupe_key for a short window

	Drain API matches CommandHelper:
	- drain_once(writer_fn)
	- drain_all(writer_fn, max_items=...)
	"""

	def __init__(self, max_size=200, min_ms_between=100, dedupe_window_ms=250, logger=None, site_tz_id="UTC"):
		self.max_size = int(max_size or 200)
		self.min_ms_between = int(min_ms_between or 0)
		self.dedupe_window_ms = int(dedupe_window_ms or 0)
		self.logger = logger
		self.site_tz_id = str(site_tz_id or "UTC")

		self._q = []
		self._last_sent_epoch = 0
		self._dedupe = {}  # dedupe_key -> last_epoch_ms

	def _now_ms(self):
		ts = clock.pack_timestamps(tz_id=self.site_tz_id)
		return int(ts.get("tsEpoch") or 0)

	def size(self):
		return len(self._q)

	def clear(self):
		self._q = []
		self._dedupe = {}
		return {"ok": True, "cleared": True}

	def enqueue(self, item):
		if len(self._q) >= self.max_size:
			return {"ok": False, "error": "queue_full", "max_size": self.max_size}

		dk = (item or {}).get("dedupe_key")
		if dk and self.dedupe_window_ms > 0:
			now = self._now_ms()
			last = int(self._dedupe.get(dk) or 0)
			if last and (now - last) <= self.dedupe_window_ms:
				return {"ok": True, "queued": False, "deduped": True, "dedupe_key": dk}
			self._dedupe[dk] = now

		self._q.append(item)
		return {"ok": True, "queued": True, "size": len(self._q)}

	def drain_once(self, writer_fn):
		if not self._q:
			return {"ok": True, "empty": True}

		now = self._now_ms()
		if self.min_ms_between > 0:
			delta = now - int(self._last_sent_epoch or 0)
			if delta < self.min_ms_between:
				return {"ok": True, "throttled": True, "wait_ms": int(self.min_ms_between - delta)}

		item = self._q.pop(0)
		self._last_sent_epoch = now

		if not callable(writer_fn):
			return {"ok": False, "error": "bad_writer_fn"}

		try:
			res = writer_fn(item)
			return {"ok": True, "ran": True, "result": res, "commandId": item.get("commandId")}
		except Exception as e:
			return {"ok": False, "ran": True, "error": str(e), "commandId": item.get("commandId")}

	def drain_all(self, writer_fn, max_items=50):
		out = []
		n = int(max_items or 50)
		for _ in range(n):
			r = self.drain_once(writer_fn)
			out.append(r)
			if r.get("empty") or r.get("throttled"):
				break
		return out