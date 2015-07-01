pdf:
	xelatex report.tex

tut:
	xelatex review_example.tex

clean:
	-rm *.aux *.lof *.log *.lot *.out *.run.xml *.toc
	-rm *~
	-rm content/*~
	-rm gfx/*~
