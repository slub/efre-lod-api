default: install

requirements:
	python3 -m pip install -r requirements.txt

install: 
	python3 -m pip install -e .
	sudo cp -v systemd/lod-api.service /etc/systemd/system/
	sudo systemctl enable lod-api

clean:
	rm -rf lod_api.egg-info

uninstall:
	python3 -m pip uninstall lod-api
	sudo rm -f /etc/systemd/system/lod-api.service
