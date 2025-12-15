# Tag mapping for commands (override per site/system)
# Keep this super small and obvious.

def _base(systemCode):
	# Example: adjust to your actual provider + base folder
	# e.g. "[default]EuroSort/A19/"
	return "[default]EuroSort/%s/" % systemCode

def chute_door_open(systemCode, dst):
	return _base(systemCode) + "Chutes/%s/DoorOpenCmd" % dst

def chute_door_close(systemCode, dst):
	return _base(systemCode) + "Chutes/%s/DoorCloseCmd" % dst

def chute_light(systemCode, dst, side=None):
	# side optional if you have per-side lights; otherwise ignore
	return _base(systemCode) + "Chutes/%s/LightCmd" % dst

def system_enable(systemCode):
	return _base(systemCode) + "System/Enable"

def system_disable(systemCode):
	return _base(systemCode) + "System/Disable"

def system_mode(systemCode):
	return _base(systemCode) + "System/Mode"

def carrier_force_release(systemCode, carrierId):
	return _base(systemCode) + "Carriers/%s/ForceRelease" % int(carrierId)