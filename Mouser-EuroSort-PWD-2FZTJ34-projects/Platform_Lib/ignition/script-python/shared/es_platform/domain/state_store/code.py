# shared/es_platform/domain/state_store.py

from shared.foundation.time import clock
from shared.es_platform.domain.shift import ShiftResolver
from shared.es_platform.domain.transitions import CarrierTransitions, ChuteTransitions
from shared.es_platform.domain.cache_api import CacheAPI
from shared.es_platform.domain.fast_update import FastUpdate
from shared.es_platform.domain.events import EventEmitter
from shared.foundation.logging.flight_recorder import FlightRecorder


class StateStore(object):
	COL_SYSTEMS = "es_platform_systems"
	COL_CARRIERS = "es_platform_carriers"
	COL_CHUTES = "es_platform_chutes"
	COL_EVENTS = "es_platform_events"

	def __init__(self, systemCode, mongo, site_tz_id="UTC", enable_cache=True, logger=None, shift_config=None, flight_config=None):
		self.systemCode = str(systemCode)
		self.mongo = mongo
		self.site_tz_id = site_tz_id
		self.enable_cache = bool(enable_cache)
		self.logger = logger

		self.shift_resolver = ShiftResolver(site_tz_id=self.site_tz_id, config=shift_config or {}, logger=logger)
		self._cache_period_key = None

		self._carriers = {}
		self._chutes = {}
		self._system = None

		self.fast = FastUpdate(self)
		self.cache = CacheAPI(self)
		self.carriers = CarrierTransitions(self)
		self.chutes = ChuteTransitions(self)
		self.events = EventEmitter(self)

		fc = dict(flight_config or {})
		self.flight = FlightRecorder(
			self.systemCode,
			base_dir=fc.get("base_dir"),
			enabled=bool(fc.get("enabled", False)),
			min_level=fc.get("min_level", "INFO"),
			min_level_when_disabled=fc.get("min_level_when_disabled", "WARN"),
			period_provider=self.shift_resolver.period_key,
			max_bytes=fc.get("max_bytes", 25 * 1024 * 1024),
			flush_each_write=bool(fc.get("flush_each_write", True)),
			site_tz_id=self.site_tz_id,
			filename_prefix=fc.get("filename_prefix", "ES_Platform")
		)

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

	def _fr(self, level, message, payload=None, eventType=None, entityType=None, entityId=None, userId=None, eventId=None, corrId=None):
		"""
		Flight recorder wrapper: never throw.
		"""
		try:
			if self.flight:
				return self.flight.record(
					level=level,
					message=message,
					payload=payload,
					eventType=eventType,
					entityType=entityType,
					entityId=entityId,
					userId=userId,
					eventId=eventId,
					corrId=corrId
				)
		except:
			pass
		return {"ok": True, "skipped": True}

	# ----------------------------
	# Cache period control
	# ----------------------------

	def cache_status(self):
		return {
			"enabled": self.enable_cache,
			"cache_period_key": self._cache_period_key,
			"carriers_cached": len(self._carriers or {}),
			"chutes_cached": len(self._chutes or {}),
		}

	def clear_cache(self, reason="manual"):
		self._carriers = {}
		self._chutes = {}
		self._system = None
		self._cache_period_key = None

		self._log("StateStore.clear_cache", {"systemCode": self.systemCode, "reason": reason})
		self._fr("INFO", "StateStore.clear_cache", {"reason": reason}, eventType="CACHE_CLEAR", entityType="SYSTEM", entityId=self.systemCode)

		return {"ok": True, "cleared": True, "reason": reason}

	def ensure_period_cache(self, hydrate=True, force=False):
		if not self.enable_cache:
			return {"ok": True, "cache": "disabled"}

		key = self.shift_resolver.period_key()
		if key is None:
			return {"ok": True, "cache": "unknown_period_key"}

		current = self._cache_period_key
		needs_reset = force or (current != key) or (not self._carriers and not self._chutes)

		if not needs_reset:
			return {"ok": True, "cache": "ok", "cache_period_key": current}

		prev = current
		self.clear_cache(reason="period_change" if current and current != key else "init_or_force")
		self._cache_period_key = key

		self._fr("INFO", "StateStore.period_cache_reset", {
			"prev_key": prev,
			"new_key": key,
			"hydrate": bool(hydrate),
			"force": bool(force)
		}, eventType="CACHE_PERIOD_RESET", entityType="SYSTEM", entityId=self.systemCode)

		if hydrate:
			h = self.hydrate_from_mongo()
			h["cache_period_key"] = key
			return h

		return {"ok": True, "cache": "reset_no_hydrate", "cache_period_key": key}

	# ----------------------------
	# Initialization
	# ----------------------------

	def initialize(self, params, layout=None, hydrate_cache=True, force=False):
		p = dict(params or {})
		ts = clock.pack_timestamps(tz_id=self.site_tz_id)

		num_carriers = int(p.get("num_of_carriers") or 0)
		if num_carriers <= 0:
			raise ValueError("initialize requires params['num_of_carriers'] > 0")

		system_version = int(p.get("system_version") or 1)

		sys_doc = self._build_system_doc(p, ts, system_version)
		self._upsert_system(sys_doc, force=force)

		chute_ids = self._resolve_chute_ids(p, layout)

		self._upsert_carriers(num_carriers, ts, force=force)
		self._upsert_chutes(chute_ids, ts, force=force)

		if hydrate_cache and self.enable_cache:
			self.ensure_period_cache(hydrate=False, force=True)
			self.hydrate_from_mongo()

		self._fr("INFO", "StateStore.initialize", {
			"num_carriers": num_carriers,
			"num_chutes": len(chute_ids),
			"force": bool(force),
			"hydrate_cache": bool(hydrate_cache),
			"params": p
		}, eventType="SYSTEM_INIT", entityType="SYSTEM", entityId=self.systemCode)

		return {
			"ok": True,
			"systemCode": self.systemCode,
			"num_carriers": num_carriers,
			"num_chutes": len(chute_ids),
			"cache_enabled": self.enable_cache,
			"cache_period_key": self._cache_period_key,
			"hydrated": bool(hydrate_cache and self.enable_cache),
			"ts": ts
		}

	def _build_system_doc(self, params, ts, system_version):
		return {
			"_id": self.systemCode,
			"systemCode": self.systemCode,
			"entityClass": "SORTER_SYSTEM",
			"system_version": system_version,
			"device_id": params.get("device_id"),
			"params": params,
			"createdAtUtc": ts.get("tsUtc"),
			"createdAtLocal": ts.get("tsLocal"),
			"createdAtEpoch": ts.get("tsEpoch"),
			"tzId": ts.get("tzId"),
			"updatedAtEpoch": ts.get("tsEpoch"),
		}

	def _resolve_chute_ids(self, params, layout):
		if isinstance(layout, dict):
			explicit = layout.get("chutes")
			if isinstance(explicit, (list, tuple)) and explicit:
				return [str(x) for x in explicit]

		sides = params.get("sides") or ["A", "B"]
		sides = [str(s) for s in sides]

		multi_lvl = bool(params.get("multi_lvl"))
		div = int(params.get("div") or 0)
		gate = bool(params.get("gate"))

		stations = int(params.get("stations") or 0)
		station_start = int(params.get("station_start") or 1)

		if stations <= 0:
			self._log("StateStore.initialize: no stations provided; chute list empty (provide layout['chutes'] or params['stations'])", params, level="warn")
			self._fr("WARN", "StateStore.initialize no_stations", {"params": params}, eventType="SYSTEM_INIT_WARN", entityType="SYSTEM", entityId=self.systemCode)
			return []

		levels = ["1", "2"] if multi_lvl else ["1"]
		if div == 0:
			dests = ["1"]
		elif div == 1:
			dests = ["1", "2"]
		else:
			dests = ["1", "3", "2"]

		if gate:
			dests = list(dests) + ["G"]

		out = []
		for i in range(station_start, station_start + stations):
			station = _z4(i)
			for lvl in levels:
				for dest in dests:
					for side in sides:
						out.append("DST-%s-%s-%s-%s" % (station, lvl, dest, side))
		return out

	def _upsert_system(self, sys_doc, force=False):
		existing = self.mongo.find_one(self.COL_SYSTEMS, {"_id": self.systemCode})
		if existing and not force:
			for k in ("createdAtUtc", "createdAtLocal", "createdAtEpoch", "tzId"):
				if k in existing and existing.get(k) is not None:
					sys_doc[k] = existing.get(k)

		self.mongo.update_one(self.COL_SYSTEMS, {"_id": self.systemCode}, {"$set": sys_doc}, upsert=True)
		self._system = sys_doc

	def _upsert_carriers(self, num_carriers, ts, force=False):
		existing_by_cid = {}

		if not force:
			try:
				existing = self.mongo.find(self.COL_CARRIERS, {"systemCode": self.systemCode}) or []
				for doc in existing:
					try:
						cid = int(doc.get("carrierId") or 0)
						if cid > 0:
							existing_by_cid[cid] = doc
					except:
						pass
			except Exception as e:
				self._log("StateStore._upsert_carriers bulk find failed", {"err": str(e)}, level="warn")
				self._fr("WARN", "StateStore._upsert_carriers bulk_find_failed", {"err": str(e)}, eventType="INIT_WARN", entityType="SYSTEM", entityId=self.systemCode)
				existing_by_cid = {}

		for cid in range(1, int(num_carriers) + 1):
			doc = self._build_carrier_doc(cid, ts)

			if not force:
				ex = existing_by_cid.get(cid)
				if ex:
					doc = self._merge_preserve(ex, doc, preserve_keys=[
						"recircCount",
						"attemptedDeliveryCount",
						"lastSeenAtEpoch",
						"lastLocation",
						"currentPhase",
						"assignedDest",
						"inductionDevice",
						"lastEventType",
						"lastEventId",
						"lastUserId",
						"lastEventDetails",
					])

			self.mongo.update_one(self.COL_CARRIERS, {"_id": doc["_id"]}, {"$set": doc}, upsert=True)

	def _upsert_chutes(self, chute_ids, ts, force=False):
		chute_ids = chute_ids or []
		existing_by_chute = {}

		if not force and chute_ids:
			try:
				existing = self.mongo.find(self.COL_CHUTES, {"systemCode": self.systemCode}) or []
				for doc in existing:
					cid = doc.get("chuteId")
					if cid:
						existing_by_chute[str(cid)] = doc
			except Exception as e:
				self._log("StateStore._upsert_chutes bulk find failed", {"err": str(e)}, level="warn")
				self._fr("WARN", "StateStore._upsert_chutes bulk_find_failed", {"err": str(e)}, eventType="INIT_WARN", entityType="SYSTEM", entityId=self.systemCode)
				existing_by_chute = {}

		for chuteId in chute_ids:
			chuteId = str(chuteId)
			doc = self._build_chute_doc(chuteId, ts)

			if not force:
				ex = existing_by_chute.get(chuteId)
				if ex:
					doc = self._merge_preserve(ex, doc, preserve_keys=[
						"enabled",
						"faulted",
						"occupied",
						"occupancyCount",
						"assignedName",
						"assignedMode",
						"lastCarrierId",
						"lastIbn",
						"lastOrder",
						"lastEventType",
						"lastEventId",
						"lastUserId",
						"lastEventDetails",
					])

			self.mongo.update_one(self.COL_CHUTES, {"_id": doc["_id"]}, {"$set": doc}, upsert=True)

	def _build_carrier_doc(self, carrierId, ts):
		return {
			"_id": self._carrier_pk(carrierId),
			"systemCode": self.systemCode,
			"entityClass": "SORTER_CARRIER",
			"carrierId": int(carrierId),
			"currentPhase": "EMPTY",
			"assignedDest": None,
			"inductionDevice": None,
			"recircCount": 0,
			"attemptedDeliveryCount": 0,
			"lastLocation": None,
			"lastSeenAtEpoch": None,
			"lastEventType": None,
			"lastEventId": None,
			"lastUserId": None,
			"lastEventDetails": None,
			"createdAtUtc": ts.get("tsUtc"),
			"createdAtLocal": ts.get("tsLocal"),
			"createdAtEpoch": ts.get("tsEpoch"),
			"tzId": ts.get("tzId"),
			"updatedAtEpoch": ts.get("tsEpoch"),
		}

	def _build_chute_doc(self, chuteId, ts):
		parsed = parse_dst(chuteId)
		return {
			"_id": self._chute_pk(chuteId),
			"systemCode": self.systemCode,
			"entityClass": "SORTER_CHUTE",
			"chuteId": str(chuteId),
			"station": parsed.get("station"),
			"level": parsed.get("level"),
			"dest": parsed.get("dest"),
			"side": parsed.get("side"),
			"enabled": True,
			"faulted": False,
			"occupied": False,
			"assignedName": None,
			"assignedMode": None,
			"lastCarrierId": None,
			"lastIbn": None,
			"lastOrder": None,
			"occupancyCount": 0,
			"lastEventType": None,
			"lastEventId": None,
			"lastUserId": None,
			"lastEventDetails": None,
			"createdAtUtc": ts.get("tsUtc"),
			"createdAtLocal": ts.get("tsLocal"),
			"createdAtEpoch": ts.get("tsEpoch"),
			"tzId": ts.get("tzId"),
			"updatedAtEpoch": ts.get("tsEpoch"),
		}

	# ----------------------------
	# Cache hydration
	# ----------------------------

	def hydrate_from_mongo(self):
		if not self.enable_cache:
			return {"ok": True, "hydrated": False, "reason": "cache_disabled"}

		sys_doc = self.mongo.find_one(self.COL_SYSTEMS, {"_id": self.systemCode})
		self._system = sys_doc

		carriers = self.mongo.find(self.COL_CARRIERS, {"systemCode": self.systemCode}) or []
		chutes = self.mongo.find(self.COL_CHUTES, {"systemCode": self.systemCode}) or []

		self._carriers = {}
		for c in carriers:
			try:
				self._carriers[int(c.get("carrierId"))] = c
			except:
				pass

		self._chutes = {}
		for ch in chutes:
			cid = ch.get("chuteId")
			if cid:
				self._chutes[str(cid)] = ch

		self._fr("INFO", "StateStore.hydrate_from_mongo", {
			"num_carriers": len(self._carriers),
			"num_chutes": len(self._chutes),
			"cache_period_key": self._cache_period_key
		}, eventType="CACHE_HYDRATE", entityType="SYSTEM", entityId=self.systemCode)

		return {"ok": True, "hydrated": True, "num_carriers": len(self._carriers), "num_chutes": len(self._chutes)}

	# ----------------------------
	# Get-or-create helpers
	# ----------------------------

	def get_or_create_carrier(self, carrierId):
		cid = int(carrierId)

		if self.enable_cache:
			self.ensure_period_cache(hydrate=True)
			doc = self._carriers.get(cid)
			if doc is not None:
				return doc

		doc = self.mongo.find_one(self.COL_CARRIERS, {"_id": self._carrier_pk(cid)})
		if doc:
			if self.enable_cache:
				self._carriers[cid] = doc
			return doc

		ts = clock.pack_timestamps(tz_id=self.site_tz_id)
		doc = self._build_carrier_doc(cid, ts)
		self.mongo.update_one(self.COL_CARRIERS, {"_id": doc["_id"]}, {"$set": doc}, upsert=True)

		if self.enable_cache:
			self._carriers[cid] = doc

		self._fr("WARN", "Late create carrier", {"carrierId": cid}, eventType="LATE_CREATE", entityType="CARRIER", entityId=cid)

		return doc

	def get_or_create_chute(self, chuteId):
		chuteId = str(chuteId)

		if self.enable_cache:
			self.ensure_period_cache(hydrate=True)
			doc = self._chutes.get(chuteId)
			if doc is not None:
				return doc

		doc = self.mongo.find_one(self.COL_CHUTES, {"_id": self._chute_pk(chuteId)})
		if doc:
			if self.enable_cache:
				self._chutes[chuteId] = doc
			return doc

		ts = clock.pack_timestamps(tz_id=self.site_tz_id)
		doc = self._build_chute_doc(chuteId, ts)
		self.mongo.update_one(self.COL_CHUTES, {"_id": doc["_id"]}, {"$set": doc}, upsert=True)

		if self.enable_cache:
			self._chutes[chuteId] = doc

		self._fr("WARN", "Late create chute", {"chuteId": chuteId}, eventType="LATE_CREATE", entityType="CHUTE", entityId=chuteId)

		return doc

	# ----------------------------
	# Minimal APIs used by CommandHelper today
	# ----------------------------

	def upsert_carrier(self, carrierId, fields=None, inc=None, on_insert=None):
		if self.enable_cache:
			self.ensure_period_cache(hydrate=True)

		cid = int(carrierId)
		r = self.fast.carrier_update(cid, set_fields=fields, inc_fields=inc, set_on_insert=on_insert)

		# Usually too chatty to log at INFO unless you enable recorder.
		self._fr("DEBUG", "Carrier upsert", {"carrierId": cid, "set": fields, "inc": inc}, eventType="CARRIER_UPSERT", entityType="CARRIER", entityId=cid)

		return r

	def chute_mark_event(self, chuteId, eventType, details=None, userId=None, eventId=None):
		if self.enable_cache:
			self.ensure_period_cache(hydrate=True)

		chuteId = str(chuteId)
		ts = clock.pack_timestamps(tz_id=self.site_tz_id)

		fields = {
			"lastEventType": str(eventType),
			"lastEventId": eventId,
			"lastUserId": userId,
			"lastEventDetails": details,
		}

		self.fast.chute_update(chuteId, set_fields=fields, inc_fields=None)

		# Always push a flight line for chute events (this is the gold)
		self._fr("INFO", "Chute event", {
			"chuteId": chuteId,
			"eventType": str(eventType),
			"details": details
		}, eventType=str(eventType), entityType="CHUTE", entityId=chuteId, userId=userId, eventId=eventId, corrId=eventId)

		# Optional: append to a small events collection for “recent events”
		try:
			ev = {
				"systemCode": self.systemCode,
				"entityClass": "CHUTE_EVENT",
				"chuteId": chuteId,
				"eventType": str(eventType),
				"eventId": eventId,
				"userId": userId,
				"details": details,
				"tsEpoch": ts.get("tsEpoch"),
				"tsLocal": ts.get("tsLocal"),
				"tsUtc": ts.get("tsUtc"),
				"tzId": ts.get("tzId"),
			}
			self.mongo.insert_one(self.COL_EVENTS, ev)
		except:
			pass

	# ----------------------------
	# Private helpers
	# ----------------------------

	def _carrier_pk(self, carrierId):
		return "%s:CARRIER:%d" % (self.systemCode, int(carrierId))

	def _chute_pk(self, chuteId):
		return "%s:CHUTE:%s" % (self.systemCode, str(chuteId))

	def _merge_preserve(self, existing, baseline, preserve_keys):
		out = dict(baseline)

		for k in (preserve_keys or []):
			if k in existing:
				out[k] = existing.get(k)

		for k in ("createdAtUtc", "createdAtLocal", "createdAtEpoch", "tzId"):
			if k in existing and existing.get(k) is not None:
				out[k] = existing.get(k)

		return out


def parse_dst(dst):
	s = str(dst or "")
	out = {"station": None, "level": None, "dest": None, "side": None}

	try:
		parts = s.split("-")
		if len(parts) >= 5 and parts[0] == "DST":
			out["station"] = parts[1]
			out["level"] = parts[2]
			out["dest"] = parts[3]
			out["side"] = parts[4]
	except:
		pass

	return out


def _z4(n):
	n = int(n)
	s = str(n)
	while len(s) < 4:
		s = "0" + s
	return s