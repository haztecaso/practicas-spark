all: copy run pdf

run:
	ssh clus-01 "~/bicimad.py"

copy:
	rsync -aPzzv . clus-01:/home/alattes/bicimad/ --exclude __pycache__
	ssh clus-01 'chmod 700 -R /home/alattes/bicimad'

pdf:
	pandoc readme.md --template eisvogel -o readme.tex
	pdflatex readme.tex

watch-pdf:
	echo 'readme.md' | entr make pdf

watch-pdf-nix:
	nix-shell -p entr --run 'make watch-pdf'
