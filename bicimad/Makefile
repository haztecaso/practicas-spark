all: copy run

run:
	ssh clus-01 "~/bicimad.py"

copy:
	rsync -aPzzv . clus-01:/home/alattes/bicimad/ --exclude __pycache__
	ssh clus-01 'chmod 700 -R /home/alattes/bicimad'

watch:
	nix-shell -p entr tree --run 'find . | entr make copy'
