# shared/es_platform/domain/cache_api.py
# Cache-first getters + common query helpers (fast during shift)

class CacheAPI(object):
	def __init__(self, store):
		self.store = store

	# ----------------------------
	# Cache-first getters
	# ----------------------------

	def get_system(self, prefer_cache=True):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		if prefer_cache and self.store._system is not None:
			return self.store._system

		doc = self.store.mongo.find_one(self.store.COL_SYSTEMS, {"_id": self.store.systemCode})
		if self.store.enable_cache and doc is not None:
			self.store._system = doc
		return doc

	def get_carrier(self, carrierId, prefer_cache=True):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		cid = int(carrierId)

		if prefer_cache and self.store.enable_cache:
			doc = self.store._carriers.get(cid)
			if doc is not None:
				return doc

		pk = self.store._carrier_pk(cid)
		doc = self.store.mongo.find_one(self.store.COL_CARRIERS, {"_id": pk})
		if self.store.enable_cache and doc is not None:
			self.store._carriers[cid] = doc
		return doc

	def get_chute(self, chuteId, prefer_cache=True):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		chuteId = str(chuteId)

		if prefer_cache and self.store.enable_cache:
			doc = self.store._chutes.get(chuteId)
			if doc is not None:
				return doc

		pk = self.store._chute_pk(chuteId)
		doc = self.store.mongo.find_one(self.store.COL_CHUTES, {"_id": pk})
		if self.store.enable_cache and doc is not None and doc.get("chuteId"):
			self.store._chutes[str(doc.get("chuteId"))] = doc
		return doc

	# ----------------------------
	# Cache-first list helpers
	# ----------------------------

	def list_carriers(self, prefer_cache=True):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		if prefer_cache and self.store.enable_cache and self.store._carriers:
			# return copies to prevent accidental mutation
			return list(self.store._carriers.values())

		rows = self.store.mongo.find(self.store.COL_CARRIERS, {"systemCode": self.store.systemCode}) or []
		if self.store.enable_cache:
			self.store._carriers = {}
			for c in rows:
				try:
					self.store._carriers[int(c.get("carrierId"))] = c
				except:
					pass
		return rows

	def list_chutes(self, prefer_cache=True):
		if self.store.enable_cache:
			self.store.ensure_period_cache(hydrate=True)

		if prefer_cache and self.store.enable_cache and self.store._chutes:
			return list(self.store._chutes.values())

		rows = self.store.mongo.find(self.store.COL_CHUTES, {"systemCode": self.store.systemCode}) or []
		if self.store.enable_cache:
			self.store._chutes = {}
			for ch in rows:
				cid = ch.get("chuteId")
				if cid:
					self.store._chutes[str(cid)] = ch
		return rows

	# ----------------------------
	# Common routing-support queries (fast)
	# ----------------------------

	def list_open_chutes(self, prefer_cache=True, require_enabled=True, require_not_faulted=True, require_not_occupied=True, dest=None, side=None, level=None, station_prefix=None):
		"""
		Return chutes that are eligible for assignment.

		filters:
		- dest: "1"/"2"/"3"/"G"
		- side: "A"/"B"/"C"
		- level: "1"(lower)/"2"(upper)
		- station_prefix: "00" or "0001" etc (string startswith)
		"""
		rows = self.list_chutes(prefer_cache=prefer_cache)

		out = []
		for ch in rows:
			if not isinstance(ch, dict):
				continue

			if require_enabled and not bool(ch.get("enabled", True)):
				continue
			if require_not_faulted and bool(ch.get("faulted", False)):
				continue
			if require_not_occupied and bool(ch.get("occupied", False)):
				continue

			if dest is not None and str(ch.get("dest")) != str(dest):
				continue
			if side is not None and str(ch.get("side")) != str(side):
				continue
			if level is not None and str(ch.get("level")) != str(level):
				continue

			if station_prefix is not None:
				st = ch.get("station")
				if st is None or (not str(st).startswith(str(station_prefix))):
					continue

			out.append(ch)

		return out

	def find_chute_by_assigned_name(self, assignedName, prefer_cache=True):
		"""
		Find all chutes currently assigned to a bucket label (e.g., PC1).
		"""
		name = None if assignedName is None else str(assignedName)
		if not name:
			return []

		rows = self.list_chutes(prefer_cache=prefer_cache)

		out = []
		for ch in rows:
			try:
				if str(ch.get("assignedName") or "") == name:
					out.append(ch)
			except:
				pass
		return out

	def find_carriers_by_phase(self, phase, prefer_cache=True):
		ph = None if phase is None else str(phase)
		if not ph:
			return []

		rows = self.list_carriers(prefer_cache=prefer_cache)
		out = []
		for c in rows:
			try:
				if str(c.get("currentPhase") or "") == ph:
					out.append(c)
			except:
				pass
		return out

	def find_carriers_assigned_to(self, chuteId, prefer_cache=True):
		dst = None if chuteId is None else str(chuteId)
		if not dst:
			return []

		rows = self.list_carriers(prefer_cache=prefer_cache)
		out = []
		for c in rows:
			try:
				if str(c.get("assignedDest") or "") == dst:
					out.append(c)
			except:
				pass
		return out