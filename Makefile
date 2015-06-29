pdf:
	xelatex report.tex

clean:
	-rm report.aux report.lof report.log report.lot report.out report.run.xml report.toc
	-rm *~
	-rm content/*~
	-rm gfx/*~
