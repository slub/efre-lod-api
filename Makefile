User?=lod

default:
	@echo "Using User: $(User) for setting up lod-api by:"
	@echo "Run 'make install' for installation."
	@echo "Run 'make uninstall' for uninstallation."

requirements:
	su $(User) -c "python3 -m pip install -r requirements.txt"

install: 
	su $(User) -c "python3 -m pip install -e ."
	[ ! -d /etc/systemd/system ] || install -Dm644 systemd/lod-api@.service /etc/systemd/system/lod-api@$(User).service

clean:
	rm -rf lod_api.egg-info

uninstall:
	su $(User) -c "python3 -m pip uninstall lod-api"
	rm -f /etc/systemd/system/lod-api.service
