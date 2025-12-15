# shared/es_platform/commands/queue.py
# Simple command queue with throttle + dedupe (Jython-safe)

from shared.foundation.time import clock


class CommandQueue(object):
	def __init__(self, max_size=200, min_ms_between=100, dedupe_window_ms=250, logger=None, site_tz_id="UTC"):
		self.max_size = int(max_size)
		self.min_ms_between = int(min_ms_between)
		self.dedupe_window_ms = int(dedupe_window_ms)
		self.logger = logger
		self.site_tz_id = str(site_tz_id or "UTC")

		self._q = []				# list of items
		self._last_sent_epoch = 0	# last drain send time (epoch ms)
		self._recent = {}			# dedupe_key -> last_enqueue_epoch (ms)

	def _now_ms(self):
		ts = clock.pack_timestamps(tz_id=self.site_tz_id)
		return int(ts.get("tsEpoch") or 0)

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

	def size(self):
		return len(self._q)

	def snapshot(self, limit=20):
		out = []
		for i, item in enumerate(self._q[:int(limit)]):
			out.append({
				"idx": i,
				"commandId": item.get("commandId"),
				"eventType": item.get("eventType"),
				"dedupe_key": item.get("dedupe_key"),
				"queuedAtEpoch": item.get("queuedAtEpoch"),
			})
		return out

	def enqueue(self, item):
		"""
		item: dict, should include:
		- commandId
		- eventType
		- writes
		- userId
		- dedupe_key (optional)
		"""
		if not isinstance(item, dict):
			raise ValueError("queue.enqueue expects dict item")

		now = self._now_ms()
		dk = item.get("dedupe_key")

		# Dedupe: skip if same dedupe_key was enqueued very recently
		if dk:
			last = self._recent.get(dk)
			if last and (now - int(last)) <= self.dedupe_window_ms:
				return {"ok": True, "queued": False, "deduped": True, "dedupe_key": dk, "size": self.size()}

		# Size cap: drop oldest
		dropped = None
		if self.size() >= self.max_size:
			try:
				dropped = self._q.pop(0)
			except:
				dropped = None

		item = dict(item)
		item["queuedAtEpoch"] = now
		self._q.append(item)

		if dk:
			self._recent[dk] = now

		return {
			"ok": True,
			"queued": True,
			"deduped": False,
			"dropped": dropped.get("commandId") if isinstance(dropped, dict) else None,
			"size": self.size()
		}

	def _can_send(self):
		now = self._now_ms()
		if self._last_sent_epoch <= 0:
			return True
		return (now - self._last_sent_epoch) >= self.min_ms_between

	def drain_once(self, writer_fn):
		"""
		writer_fn(item) -> result dict (or raises)
		"""
		if self.size() <= 0:
			return {"ok": True, "drained": 0, "empty": True}

		if not self._can_send():
			return {"ok": True, "drained": 0, "throttled": True, "min_ms_between": self.min_ms_between}

		item = self._q.pop(0)
		self._last_sent_epoch = self._now_ms()

		try:
			res = writer_fn(item)
			return {"ok": True, "drained": 1, "result": res, "commandId": item.get("commandId")}
		except Exception as e:
			# Put it back at the front? usually no; we push to back for retry-style behavior
			item["lastError"] = str(e)
			item["lastErrorAtEpoch"] = self._now_ms()
			self._q.append(item)
			return {"ok": False, "drained": 1, "error": str(e), "commandId": item.get("commandId")}

	def drain_all(self, writer_fn, max_items=50):
		n = 0
		results = []
		max_items = int(max_items)

		while n < max_items and self.size() > 0:
			r = self.drain_once(writer_fn)
			results.append(r)
			if r.get("throttled"):
				break
			n += 1

		return {"ok": True, "attempted": n, "remaining": self.size(), "results": results}