try:
	import system
except:
	system = None


class QueueRunner(object):
	def __init__(self, command_helper, interval_ms=100, max_per_tick=1, name="ES_CommandQueueRunner"):
		self.cmd = command_helper
		self.interval_ms = int(interval_ms)
		self.max_per_tick = int(max_per_tick)
		self.name = str(name)
		self._running = False

	def is_running(self):
		return bool(self._running)

	def stop(self):
		self._running = False
		return {"ok": True, "stopped": True, "name": self.name}

	def start(self):
		if self._running:
			return {"ok": True, "started": False, "reason": "already_running", "name": self.name}

		if system is None:
			raise RuntimeError("QueueRunner requires Gateway scope (system not available).")

		self._running = True

		def _loop():
			while self._running:
				try:
					self.cmd.drain_queue_all(max_items=self.max_per_tick)
				except Exception as e:
					try:
						print("QueueRunner error:", e)
					except:
						pass
				system.util.sleep(self.interval_ms)

		system.util.invokeAsynchronous(_loop, self.name)
		return {"ok": True, "started": True, "name": self.name, "interval_ms": self.interval_ms, "max_per_tick": self.max_per_tick}