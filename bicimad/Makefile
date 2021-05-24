all: copy run

run:
	ssh clus-01 "~/bicimad.py"

copy:
	rsync -aPzzv ./*.py clus-01:/home/alattes/

watch:
	nix-shell -p entr --run 'ls | entr make copy'
